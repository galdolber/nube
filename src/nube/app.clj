(ns nube.app
  (:require [bidi.bidi :refer [match-route]]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [org.httpkit.server :refer [with-channel send!]]
            [pandect.core :refer [sha1]]))

(def env (atom {:controller "localhost"
                :redis-host "127.0.0.1"
                :redis-port "6379"
                :docker-port "4243"}))

(defn redis-conf []
  {:host (:redis-host @env)
   :port (read-string (:redis-port @env))
   :password (:redis-password @env)})

(defmacro redis! [& body] `(car/wcar (redis-conf) ~@body))
(def port-range (set (range 8000 8999)))
(def router (atom {:instances {} :envs {} :unhealthy #{}}))

(defn mark-host-health [host healthy]
  (swap! router update-in [:unhealthy] #((if healthy disj conj) % host))
  healthy)

(defn docker!
  ([method host uri] (docker! method host uri {}))
  ([method host uri options]
     (let [res @((if (= :get method) http/get http/post) (str "http://" host ":" (:docker-port @env) "/" uri) options)
           status (:status res)]
       (if (and status (< 199 status 300))
          (let [body (:body res)]
            (if (clojure.string/blank? body)
              {}
              (json/read-str body :key-fn keyword)))
          (throw (Exception. (str "Docker remote api error. Status: " status)))))))

(defn ssplit [s] (when s (clojure.string/split s #":")))
(defn create-token [] (let [token (sha1 (pr-str (java.util.Date.)))] (redis! (car/set :token token)) token))
(defn get-token [] (if-let [token (redis! (car/get :token))] token (create-token)))

(defn notify-routers [] (redis! (car/publish "updates" (java.util.Date.))))

(defn load-apps [] (redis! (car/smembers :apps)))
(defn add-app [app] (redis! (car/sadd :apps app)))
(defn remove-app [app] (redis! (car/srem :apps app)))

(defn add-app-env [app env] (redis! (car/sadd (str app ":envs") env)))
(defn remove-app-env [app env] (redis! (car/srem (str app ":envs") env)))
(defn load-app-envs [app] (redis! (car/smembers (str app ":envs"))))

(defn load-app-instances [app] (redis! (car/smembers (str app ":instances"))))
(defn add-app-instance [app instance] (redis! (car/sadd (str app ":instances") instance) (notify-routers)))
(defn remove-app-instance [app instance] (redis! (car/srem (str app ":instance") instance) (notify-routers)))

(defn load-hosts [] (redis! (car/smembers :hosts)))
(defn add-host [host] (redis! (car/sadd :hosts host)))
(defn remove-host [host] (redis! (car/srem :hosts host)))

(defn load-deployments [app] (redis! (car/lrange (str "deployments:" app) -100 -1)))
(defn save-deployments [app image count]
  (redis! (car/lpush (str "deployments:" app)
                     {:timestamp (java.util.Date.)
                      :app app
                      :image image
                      :count count})))

(defn get-containers [host] (docker! :get host "containers/json"))

(defn run-container [host port image envs]
  (let [internal-port "80/tcp"
        options {:Hostname "" :User "" :AttachStdin false :AttachStdout true :AttachStderr true
                 :Tty true :OpenStdin false :StdinOnce false :Cmd nil :Volumes {} :Env envs
                 :Image image :ExposedPorts {internal-port {}}}
        start-options {:PortBindings {"80/tcp" [{:HostPort (str port)}]}}
        id (:Id (docker! :post host "containers/create"
                         {:headers {"Content-Type" "application/json"}
                          :body (json/write-str options)}))]
    (println "Container with id" id "created.")
    (docker! :post host (str "containers/" id "/start")
             {:headers {"Content-Type" "application/json"}
              :body (json/write-str start-options)})
    id))

(defn init-routing-table []
  (doseq [app (load-apps)]
    (swap! router assoc-in [:instances app] (load-app-instances app))))

(defn load-ports-in-use [host]
  (set (mapv #(:PublicPort (first (:Ports %))) (get-containers host))))

(defn find-available-port [host]
  (rand-nth (vec (clojure.set/difference port-range (load-ports-in-use host)))))

(defn load-container-by-host-and-port [host port]
  (first (filter #(= port (:PublicPort (first (:Ports %)))) (get-containers host))))

(defn stop-container [host id]
  (docker! :post host (str "containers/" id "/stop")))

(defn stop-container-by-port [host port]
  (stop-container host (:Id (load-container-by-host-and-port host port))))

(defn health-check-host [host port]
  (mark-host-health host
   (loop [n 10]
     (when (pos? n)
       (if (= 200 (:status @(http/get (str "http://" host ":" port "/") {:timeout 5000})))
         true
         (recur (dec n)))))))

(defn health-check-instances []
  (doseq [i (vals (:instances @router))]
    (health-check-host i 80)))

(defn pull-docker-image [host image]
  (let [[image tag] (ssplit image)]
    (docker! :post host (str "images/create?fromImage=" image (if tag (str "&tag=" tag) "")))))

(defn kill-app-instance [app host port]
  (println "Killing instance at" (str host ":" port))
  (stop-container-by-port host port)
  (remove-app-instance app (str host ":" port)))

(defn deploy-app-instance [app host port image]
  (println "Pulling new tags for" image)
  (pull-docker-image host image)
  (println "Starting new container at" (str host ":" port))
  (let [id (run-container host port image (load-app-envs app))]
    (println "Checking host health")
    (if-not (health-check-host host port)
      (do
        (stop-container host id)
        (throw (Exception. "Failed to deploy new instance.")))
      (try
        (println "Adding" (str host ":" port) "to router")
        (add-app-instance app (str host ":" port))
        (catch Exception e
          (println "Deploy failed. Rolling back.")
          (try (kill-app-instance app host port)
               (throw (Exception. "Deployment failed. Rolling back."))
               (catch Exception e (throw (Exception. "Rollback failed. System may be in an invalid state.")))))))))

(defn count-running-containers [host] (count (get-containers host)))

(defn count-all-running-containers []
  (into {} (map #(vector % (count-running-containers %)) (load-hosts))))

(defn get-container-distribution []
  (into {} (map #(vector % (count (get-containers %))) (load-hosts))))

(defn deploy-new-app-instances [app image count]
  (let [count (if (string? count) (read-string count) count)
        dist (get-container-distribution)
        total-containers (apply + (map second dist))
        hosts (vec (keys dist))
        total-hosts (clojure.core/count hosts)
        ideal-count-per-host (Math/ceil (/ (+ total-containers count) total-hosts))
        launching (loop [count count launching (reduce #(assoc % %2 0) {} hosts)]
                    (if (zero? count)
                      launching
                      (let [host (hosts (mod count total-hosts))]
                        (if (< (dist host) ideal-count-per-host)
                          (recur (dec count) (update-in launching [host] inc))
                          launching))))]
    (doseq [host hosts]
      (dotimes [n (launching host)]
        (deploy-app-instance app host (find-available-port host) image)))))

(defn deploy-app-instances [app image count]
  (println "Deploying" count app "with" image)
  (let [instances (load-app-instances app)]
    (deploy-new-app-instances app image count)
    (save-deployments app image count)
    (doseq [instance instances]
      (let [[image tag] (ssplit instance)]
        (kill-app-instance app image tag)))))

(defn load-app-logs [app]
  (for [instance (load-app-instances app)]
    (let [[host port] (ssplit instance)]
      (println "Loading logs for" instance)
      #_(logs (docker-cli host) (:Id (load-container-by-host-and-port host port))))))

(defn kill-app-instances [app]
  (doseq [instance (load-app-instances app)]
    (let [[host port] instance]
      kill-app-instance app host port)))

(defn describe []
  (into {} (for [app (load-apps)]
             (let [instances (load-app-instances app)]
               [app {:instances instances
                     :envs (load-app-envs app)
                     :image (when-let [[host port] (ssplit (first instances))]
                              (:Image (load-container-by-host-and-port host port)))}]))))

(def routes
  ["/" {"describe" #'describe
        "apps" {"" #'load-apps
                ["/add/" :app] #'add-app
                ["/delete/" :app] #'remove-app}
        "hosts" {"" #'load-hosts
                 ["/add/" :host] #'add-host
                 ["/delete/" :host] #'remove-host}
        [:app] {"/deploy" #'deploy-app-instances
                "/instances" #'load-app-instances
                "/history" #'load-deployments
                "/envs" {"" #'load-app-envs
                         ["/add/" :env] #'add-app-env
                         ["/delete/" :env] #'remove-app-env}
                "/logs" #'load-app-logs
                "/kill" #'kill-app-instance}}])

(defn controller-app [{:keys [params uri] :as req}]
  (if-let [{:keys [handler route-params] :as all} (match-route routes uri)]
    (let [params (merge (into {} (map (fn [[k v]] [(keyword k) v]) params)) route-params)
          fn-params (mapv params (map (comp keyword name) (first (:arglists (meta handler)))))]
      (try
        {:status 200
         :headers {"Content-Type" "text/plain"}
         :body (pr-str (apply (var-get handler) fn-params))}
        (catch Exception e
          (.printStackTrace e)
          {:status 500
           :headers {"Content-Type" "text/plain"}
           :body (pr-str {:error (.getMessage e)})})))
    {:status 404
     :headers {"Content-Type" "text/plain"}
     :body (pr-str {:error "Page not found"})}))

(defn extract-app [req] (first (ssplit ((:headers req) "host"))))

(defn pipe [req]
  (if-let [app (extract-app req)]
    (if-let [instances (get-in @router [:instances app])]
      (if (seq instances)
        (if-let [instance (rand-nth (vec (clojure.set/difference instances (:unhealthy @router))))]
          (with-channel req channel
            (http/request
             {:url (str (name (:scheme req)) "://" instance (:uri req))
              :method (:request-method req)
              :headers (:headers req)
              :query-params (:query-params req)
              :form-params (:form-params req)
              :body (:body req)
              :basic-auth ((:headers req) "basic-auth")
              :user-agent ((:headers req) "user-agent")}
             #(send! channel {:status (:status %)
                              :body (:body %)
                              :headers (into {} (map (fn [[k v]] (vector (name k) v)) (:headers %)))})))
          {:status 503 :body (str "No backend available for " app)})
        {:status 503 :body (str "No backend available for " app)})
      {:status 404 :body (str "No backend found for " app)})
    {:status 404 :body "Invalid hostname"}))

(defn app [req]
  (if (= (:controller @env) (extract-app req))
    (controller-app req)
    (pipe req)))

(defn init []
  (init-routing-table)
  (car/with-new-pubsub-listener (redis-conf) {"updates" (fn [_] (init-routing-table))}
    (car/subscribe "updates"))
  (future
    (loop []
      (health-check-instances)
      (Thread/sleep 10000)
      (recur))))
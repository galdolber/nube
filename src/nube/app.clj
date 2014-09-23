(ns nube.app
  (:require [bidi.bidi :refer [match-route]]
            [taoensso.carmine :as car :refer [wcar]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [org.httpkit.server :refer [with-channel send!]]
            [pandect.core :refer [sha1]]
            [ring.middleware.basic-authentication :refer [basic-authentication-request authentication-failure]]))

(def env (atom {:controller "localhost"
                :port "8080"
                :redishost "127.0.0.1"
                :redisport "6379"
                :dockerport "4243"}))

(defn redis-conf []
  {:spec {:host (:redishost @env)
          :port (read-string (:redisport @env))
          :password (:redispassword @env)}})

(defmacro redis! [& body] `(car/wcar (redis-conf) ~@body))

(def port-range (set (range 8000 8999)))
(def router (atom {:instances {} :envs {} :unhealthy #{}}))

(defn mark-host-health [host healthy]
  (swap! router update-in [:unhealthy] #((if healthy disj conj) % host))
  healthy)

(defn docker!
  ([method host uri] (docker! method host uri {}))
  ([method host uri options]
     (let [res @((if (= :get method) http/get http/post) (str "http://" host ":" (:dockerport @env) "/" uri) options)
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

(defn add-app-env [app env val] (redis! (car/hset (str app ":envs") env val)))
(defn remove-app-env [app env] (redis! (car/hdel (str app ":envs") env)))
(defn load-app-envs [app] (apply hash-map (redis! (car/hgetall (str app ":envs")))))

(defn load-app-instances [app] (redis! (car/smembers (str app ":instances"))))
(defn add-app-instance [app instance] (redis! (car/sadd (str app ":instances") instance) (notify-routers)))
(defn remove-app-instance [app instance] (redis! (car/srem (str app ":instances") instance) (notify-routers)))

(defn load-pending-app-instances [app] (redis! (car/smembers (str app ":pending-instances"))))
(defn add-pending-app-instance [app instance] (redis! (car/sadd (str app ":pending-instances") instance)))
(defn remove-pending-app-instance [app instance] (redis! (car/srem (str app ":pending-instances") instance)))

(defn load-hosts [] (redis! (car/smembers :hosts)))
(defn add-host [host] (redis! (car/sadd :hosts host)))
(defn remove-host [host] (redis! (car/srem :hosts host)))

(defn load-deployments [app] (redis! (car/lrange (str "deployments:" app) -100 -1)))
(defn save-deployments [app image count]
  (redis! (car/lpush (str "deployments:" app)
                     {:timestamp (java.util.Date.)
                      :app app
                      :instances (load-pending-app-instances app)
                      :image image
                      :count count})))

(defn get-containers [host] (docker! :get host "containers/json"))

(defn run-container [host port internal-port image envs]
  (let [internal-port (or internal-port "80/tcp")
        options {:Hostname "" :User "" :AttachStdin false :AttachStdout true :AttachStderr true
                 :Tty true :OpenStdin false :StdinOnce false :Cmd nil :Volumes {}
                 :Env (mapv (fn [[k v]] (str k "=" v)) envs)
                 :Image image :ExposedPorts {internal-port {}}}
        start-options {:PortBindings {internal-port [{:HostPort (str port)}]}}
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
  (first (filter #(= port (str (:PublicPort (first (:Ports %))))) (get-containers host))))

(defn stop-container [host id]
  (docker! :post host (str "containers/" id "/stop")))

(defn stop-container-by-port [host port]
  (when-let [id (:Id (load-container-by-host-and-port host port))]
    (stop-container host id)))

(defn health-check-host [host port]
  (mark-host-health (str host ":" port)
   (loop [n 10]
     (when (pos? n)
       (if (= 200 (:status @(http/get (str "http://" host ":" port "/") {:timeout 5000})))
         true
         (do
           (Thread/sleep (* (- 10 n) 100))
           (recur (dec n))))))))

(defn health-check-instances []
  (doseq [l (vals (:instances @router))]
    (doseq [i l]
      (let [[host port] (ssplit i)]
        (health-check-host host port)))))

(defn pull-docker-image [host image]
  (let [[image tag] (ssplit image)]
    (docker! :post host (str "images/create?fromImage=" image (if tag (str "&tag=" tag) "")))))

(defn kill-app-instance [app host port]
  (println "Killing instance at" (str host ":" port))
  (stop-container-by-port host port)
  (remove-app-instance app (str host ":" port))
  (mark-host-health (str host ":" port) true))

(defn deploy-app-instance [app host port internal-port image]
  (println "Pulling new tags for" image)
  (pull-docker-image host image)
  (println "Starting new container at" (str host ":" port))
  (let [id (run-container host port internal-port image (load-app-envs app))]
    (println "Checking host health")
    (if-not (health-check-host host port)
      (do
        (stop-container host id)
        (Exception. "Failed to deploy new instance."))
      (try
        (println "Adding" (str host ":" port) "pending")
        (add-pending-app-instance app (str host ":" port))
        :ok
        (catch Exception e
          (println "Deploy failed. Rolling back.")
          (try (kill-app-instance app host port)
               (Exception. "Deployment failed. Rolling back.")
               (catch Exception e (Exception. "Rollback failed. System may be in an invalid state."))))))))

(defn deploy-new-app-instances [app image count internal-port]
  (let [count (if (string? count) (read-string count) count)
        dist (into {} (map #(vector % (clojure.core/count (get-containers %))) (load-hosts)))
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
    (let [launches (remove #(= :ok %) (pmap #(deploy-app-instance app % (find-available-port %) internal-port image)
                                            (flatten (map (fn [[h n]] (repeat n h)) launching))))]
      (when-not (empty? launches)
        (throw (first launches))))))

(defn deploy-app-instances [app image count internal-port]
  (println "Deploying" count app "with" image)
  (try
    (let [instances (load-app-instances app)]
      (deploy-new-app-instances app image count internal-port)
      (save-deployments app image count)
      (doseq [instance (load-pending-app-instances app)]
        (add-app-instance app instance)
        (remove-pending-app-instance app instance))
      (doseq [instance instances]
        (let [[image tag] (ssplit instance)]
          (kill-app-instance app image tag))))
    "App successfully deployed!"
    (catch Exception e
      (println "Rolling back deploy")
      (doseq [instance (load-pending-app-instances app)]
        (let [[host port] (ssplit instance)]          
          (stop-container-by-port host port))
        (remove-pending-app-instance app instance))
      (throw (Exception. "Deploy failed. Rolling back.")))))

(defn load-app-logs [app]
  (vec
   (for [instance (load-app-instances app)]
     (let [[host port] (ssplit instance)]
       (docker! :get host (str "containers/" (:Id (load-container-by-host-and-port host port))
                               "/logs?stderr=1&stdout=1&timestamps=1"))))))

(defn kill-app-instances [app]
  (doseq [instance (load-app-instances app)]
    (let [[host port] (ssplit instance)]
      (kill-app-instance app host port))))

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
                         ["/add/" :env "/" :val] #'add-app-env
                         ["/delete/" :env] #'remove-app-env}
                "/logs" #'load-app-logs
                "/kill" #'kill-app-instances}}])

(defn controller-app [{:keys [params uri] :as req}]
  (if-not (= (get-token) (params "x-token")) ; todo move to headers when the CLI is ready
    {:status 500
     :headers {"Content-Type" "text/plain"}
     :body (pr-str {:error "Bad token"})}
    (->> (if-let [{:keys [handler route-params] :as all} (match-route routes uri)]
           (let [params (merge (into {} (map (fn [[k v]] [(keyword k) v]) params)) route-params)
                 fn-params (mapv params (map (comp keyword name) (first (:arglists (meta handler)))))]
             (try
               {:status 200
                :headers {"Content-Type" "text/plain"}
                :body (pr-str {:data (or (apply (var-get handler) fn-params) :ok)})}
               (catch Exception e
                 (.printStackTrace e) ; todo notify
                 {:status 500
                  :headers {"Content-Type" "text/plain"}
                  :body (pr-str {:error (.getMessage e)})})))
           {:status 404
            :headers {"Content-Type" "text/plain"}
            :body (pr-str {:error "Page not found"})})
         (send! channel)
         future
         (with-channel req channel))))

(defn extract-app [req] (first (ssplit ((:headers req) "host"))))

(defn pipe-instance [req app]  
  (if-let [instances (get-in @router [:instances app])]
    (if (seq instances)
      (if-let [instance (rand-nth (vec (clojure.set/difference (set instances) (:unhealthy @router))))]
        (with-channel req channel
          (http/request
           {:url (str (name (:scheme req)) "://" instance (:uri req)
                      (let [q (:query-string req)] (if (clojure.string/blank? q) "" (str "?" q))))
            :method (:request-method req)
            :headers (:headers req)
                                        ;:form-params (:form-params req)
            :body (:body req)
            :user-agent ((:headers req) "user-agent")}
           #(send! channel {:status (:status %)
                            :body (:body %)
                            :headers (into {} (map (fn [[k v]] (vector (name k) v)) (:headers %)))})))
        {:status 503 :body (str "No backend available for " app)})
      {:status 503 :body (str "No backend available for " app)})
    {:status 404 :body (str "No backend found for " app)}))

(defn pipe [req]
  (if-let [app (extract-app req)]
    (let [envs (load-app-envs app)]
      (if (envs "auth-user")
        (let [auth-req (basic-authentication-request req #(and (= % (envs "auth-user")) (= %2 (envs "auth-password"))))]
          (if (:basic-authentication auth-req)
            (pipe-instance auth-req app)
            (authentication-failure nil nil)))
        (pipe-instance req app)))
    {:status 404 :body "Invalid hostname"}))

(defn app [req]
  (if (= (:controller @env) (extract-app req))
    (controller-app req)
    (pipe req)))

(defn init []
  (get-token)
  (init-routing-table)
  (car/with-new-pubsub-listener (:spec (redis-conf)) {"updates" (fn [_] (init-routing-table))}
    (car/subscribe "updates"))
  (future
    (loop []
      (health-check-instances)
      (Thread/sleep 5000)
      (recur))))


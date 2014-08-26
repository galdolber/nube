(ns nube.core
  (:gen-class)
  (:require
   [nube.app :refer [app init env]]
   [ring.middleware.reload :refer [wrap-reload]]
   [ring.middleware.params :refer [wrap-params]]
   [org.httpkit.server :as http-kit]))

(defn -main [& args]
  (swap! env merge (into {} (map (fn [[k v]] (vector (keyword (.toLowerCase k)) v)) (System/getenv))))
  (init)
  (http-kit/run-server (wrap-params (wrap-reload #'app)) {:port (read-string (:port @env))})
  (println "Server started"))

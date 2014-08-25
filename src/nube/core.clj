(ns nube.core
  (:gen-class)
  (:require
   [nube.app :refer [app init]]
   [ring.middleware.reload :refer [wrap-reload]]
   [ring.middleware.params :refer [wrap-params]]
   [org.httpkit.server :as http-kit]))

(defn -main [& args]
  (init)
  (http-kit/run-server (wrap-params (wrap-reload #'app)) {:port 8080})
  (println "Server started"))

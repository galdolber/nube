(defproject nube "0.1.0-SNAPSHOT"
  :description "Private docker PaaS"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot :all
  :main nube.core
  :uberjar-name "nube.jar"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [bidi "1.10.4"]
                 [pandect "0.3.4"]
                 [ring-basic-authentication "1.0.5"]
                 [com.taoensso/carmine "2.6.2"]
                 [org.clojure/data.json "0.2.5"]
                 [http-kit "2.1.18"]
                 [ring/ring-devel "1.3.0"]])

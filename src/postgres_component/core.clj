(ns postgres-component.core
  (:require [hikari-cp.core :refer [make-datasource close-datasource]]
            [clojure.java.jdbc :as jdbc]
            [com.stuartsierra.component :as component]))

(defn- make-spec [postgres]
  {:datasource (make-datasource postgres)})

(defn- default-db-spec
  "Returns the specification of the default ('postgres') database"
  [{:keys [username password server-name] :as config}]
  (let [subname (str "//" server-name "/postgres")]
    {:subprotocol "postgresql"
     :subname subname
     :user username
     :password password}))

(defn- vectorize-string [s]
  (if (string? s) [s] s))

(defn- execute-default!
  "Execute statement on the default ('postgres') database"
  ([postgres statement]
   (execute-default! postgres statement {}))

  ([postgres statement {:keys [transaction?] :or {transaction? true} :as opts}]
   (let [spec (default-db-spec postgres)
         statement (vectorize-string statement)]
    (jdbc/with-db-connection [conn spec]
      (jdbc/db-do-commands conn transaction? statement)))))

(defrecord Postgres []
  component/Lifecycle
  (start [postgres]
    (if (:spec postgres)
      postgres
      (assoc postgres
             :spec (make-spec postgres))))

  (stop [postgres]
    (if-let [datasource (-> postgres :spec :datasource)]
      (do (close-datasource datasource)
          (dissoc postgres :spec))
      postgres)))

(defn postgres [config]
  (-> config
      (assoc :adapter "postgresql")
      (map->Postgres)))

(defn create-database! [{:keys [database-name] :as postgres}]
  (execute-default! postgres (str "CREATE DATABASE " database-name)
                    {:transaction? false}))

(defn drop-database! [{:keys [database-name] :as postgres}]
  (execute-default! postgres (str "DROP DATABASE " database-name)
                    {:transaction? false}))

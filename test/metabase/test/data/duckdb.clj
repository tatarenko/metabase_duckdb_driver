(ns metabase.test.data.duckdb
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [metabase.config :as config]
   [metabase.driver :as driver]
   [metabase.driver.ddl.interface :as ddl.i]
   [metabase.driver.sql.util :as sql.u]
   [metabase.test.data.interface :as tx]
   [metabase.test.data.sql :as sql.tx]
   [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
   [metabase.test.data.sql-jdbc.execute :as execute]
   [metabase.test.data.sql-jdbc.load-data :as load-data]
   [metabase.test.data.sql.ddl :as ddl]
   [metabase.util :as u]
   [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

(sql-jdbc.tx/add-test-extensions! :duckdb)

(defmethod driver/database-supports? [:duckdb :foreign-keys] [_driver _feature _db] )
;; (doseq [[feature supported?] {:foreign-keys  (not config/is-test?)}]
;;   (defmethod driver/database-supports? [:duckdb feature] [_driver _feature _db] supported?))



(def db-connection-details
  (delay {:read_only       false
          :old_implicit_casting   true
          "temp_directory"         "metabase_test.ddb.tmp"
          :database_file           "metabase_test.ddb"
          "custom_user_agent"      "metabase_test"
          :allow_unsigned_extensions  false
          :subname                 "metabase_test.ddb"}))

(defmethod tx/dbdef->connection-details :duckdb
  [& _]
  @db-connection-details)

(doseq [[base-type db-type] {:type/BigInteger     "BIGINT"
                             :type/Boolean        "BOOL"
                             :type/Date           "DATE"
                             :type/DateTime       "TIMESTAMP"
                             :type/DateTimeWithTZ "TIMESTAMPTZ"
                             :type/Decimal        "DECIMAL"
                             :type/Float          "FLOAT"
                             :type/Integer        "INTEGER"
                             :type/Text           "STRING"
                             :type/Time           "TIME"
                             :type/UUID           "UUID"}]
  (defmethod sql.tx/field-base-type->sql-type [:duckdb base-type] [_ _] db-type))

;; (defmethod tx/create-db! :duckdb
;;   [_driver dbdef]
;;   (tx/destroy-db! _driver dbdef)
;;   (apply (get-method tx/create-db! :sql-jdbc/test-extensions) _driver dbdef))


(defmethod tx/destroy-db! :duckdb
  [_driver dbdef]
  (let [file (io/file (str (tx/escaped-database-name dbdef) ".ddb"))
        wal-file (io/file (str (tx/escaped-database-name dbdef) ".ddb.wal"))]
    (log/infof "LOUISAAAAAA Destroying DuckDB database %s" (tx/escaped-database-name dbdef))
    (when (.exists file)
      (.delete file))
    (when (.exists wal-file)
      (.delete wal-file))
    ))

(defmethod sql.tx/pk-sql-type :duckdb [_] "INTEGER")

(defmethod sql.tx/drop-db-if-exists-sql :duckdb [_ {:keys [database-name]}] (format "DETACH '%s';" database-name))
(defmethod sql.tx/create-db-sql         :duckdb [_ {:keys [database-name]}]
  (format "ATTACH '%s.ddb';" database-name))

(defmethod sql.tx/add-fk-sql            :duckdb [& _] nil)

(defmethod load-data/load-data! :duckdb [& args]
  (apply load-data/load-data-maybe-add-ids-chunked! args))

(defonce ^:private reference-load-durations
  (delay (edn/read-string (slurp "test_resources/load-durations.edn"))))


(defmethod execute/execute-sql! :sqlite [& args]
  (apply execute/sequentially-execute-sql! args))

(defmethod tx/sorts-nil-first? :duckdb
  [_driver _base-type]
  false)

(defmethod tx/create-db! :duckdb
  [driver {:keys [table-definitions] :as dbdef} & options]
  (tx/destroy-db! driver dbdef)
  ;; now execute statements to create the DB
  (doseq [statement (ddl/create-db-ddl-statements driver dbdef)]
    (execute/execute-sql! driver :server dbdef statement))
  ;; next, get a set of statements for creating the tables
  (let [statements (apply ddl/create-db-tables-ddl-statements driver dbdef options)]
    ;; exec the combined statement. Notice we're now executing in the `:db` context e.g. executing them for a specific
    ;; DB rather than on `:server` (no DB in particular)
    (execute/execute-sql! driver :db dbdef (str/join ";\n" statements)))
  ;; Now load the data for each Table
  (doseq [tabledef table-definitions
          :let     [reference-duration (or (some-> (get @reference-load-durations [(:database-name dbdef) (:table-name tabledef)])
                                                   u/format-nanoseconds)
                                           "NONE")]]
    (u/profile (format "load-data for %s %s %s (reference H2 duration: %s)"
                       (name driver) (:database-name dbdef) (:table-name tabledef) reference-duration)
               (try
                 (load-data/load-data! driver dbdef tabledef)
                 (catch Throwable e
                   (throw (ex-info (format "Error loading data: %s" (ex-message e))
                                   {:driver driver, :tabledef (update tabledef :rows (fn [rows]
                                                                                       (concat (take 10 rows) ['...])))}
                                   e)))))))

;; (defmethod sql.tx/create-table-sql :duckdb
;;   [driver {:keys [database-name]} {:keys [table-name field-definitions]}]
;;   (let [quote-name #(sql.u/quote-name driver :field (ddl.i/format-name driver %))]
;;     (format "CREATE TABLE %s (%s)"
;;             (sql.tx/qualify-and-quote driver database-name table-name)
;;             (->> field-definitions
;;                  (map (fn [{:keys [field-name base-type]}]
;;                         (format "%s %s" (quote-name field-name) (if (map? base-type)
;;                                                                   (:native base-type)
;;                                                                   (sql.tx/field-base-type->sql-type driver base-type)))))
;;                  (interpose ", ")
;;                  (apply str)))))

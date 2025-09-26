(ns metabase.driver.duckdb
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [java-time.api :as t]
   [medley.core :as m]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.log :as log])
  (:import
   (java.sql
    Connection
    PreparedStatement
    ResultSet
    ResultSetMetaData
    Statement
    Time
    Types)
   (java.time LocalDate LocalTime OffsetTime)
   (java.time.temporal ChronoField)
   (java.util.concurrent ConcurrentHashMap)
   (java.util.concurrent.atomic AtomicBoolean)
   (java.util.concurrent.locks ReentrantLock)))

(set! *warn-on-reflection* true)

;; Thread-safe tracking of init SQL execution per database connection
(def ^:private ^ConcurrentHashMap init-sql-states (ConcurrentHashMap.))
(def ^:private ^ReentrantLock init-sql-lock (ReentrantLock.))


;; Generate a unique key for a database connection based on its id + connection details,
;; so when connection details change, the key changes and the init SQL is executed again
(defn- get-database-key
  [db-or-id-or-spec]
  (let [details (if (map? (:details db-or-id-or-spec))
                  (:details db-or-id-or-spec)
                  db-or-id-or-spec)
        id (get db-or-id-or-spec :id)]
    (assoc details :id id)))

(driver/register! :duckdb, :parent :sql-jdbc)

(doseq [[feature supported?] {:metadata/key-constraints      false  ;; fetching metadata about foreign key constraints is not supported, but JOINs generally are.
                              :upload-with-auto-pk           false
                              :datetime-diff                 true
                              :schemas                       false}]
  (defmethod driver/database-supports? [:duckdb feature] [_driver _feature _db] supported?))

(defmethod sql-jdbc.conn/data-source-name :duckdb
  [_driver details]
  ((some-fn :database_file)
   details))

(def premium-features-namespace
  (try
    (require '[metabase.premium-features.core :as premium-features])    ;; For Metabase 0.52 or after
    'metabase.premium-features.core
    (catch Exception _
      (try
        (require '[metabase.public-settings.premium-features :as premium-features])   ;; For Metabase < 0.52
        'metabase.public-settings.premium-features
        (catch Exception e
          (throw (ex-info "Could not load either premium features namespace"
                          {:error e})))))))

(defn- is-hosted?  []
  (let [premium-feature-ns (find-ns premium-features-namespace)]
    ((ns-resolve premium-feature-ns 'is-hosted?))))

(defn- get-motherduck-token [details-map]
  (try
    ;; For Metabase 0.55 or after
    ((requiring-resolve 'metabase.secrets.models.secret/value-as-string) :duckdb details-map "motherduck_token")
    (catch Exception _
      (try
        ;; For Metabase < 0.55
        ((requiring-resolve 'metabase.models.secret/value-as-string) :duckdb details-map "motherduck_token")
        (catch Exception _
          ;; For Metabase < 0.52
          (or (-> ((requiring-resolve 'metabase.models.secret/db-details-prop->secret-map) details-map "motherduck_token")
                  ((requiring-resolve 'metabase.models.secret/value->string)))
              ((requiring-resolve 'metabase.models.secret/get-secret-string) details-map "motherduck_token")))))))

(defn- database-file-path-split [database_file]
  (let [url-parts (str/split database_file #"\?")]
    (if (= 2 (count url-parts))
      (let [database-file (first url-parts)
            additional-options (second url-parts)]
        [database-file additional-options])
      [database_file ""])))

(defn- jdbc-spec
  "Creates a spec for `clojure.java.jdbc` to use for connecting to DuckDB via JDBC from the given `opts`"
  [{:keys [database_file, read_only, allow_unsigned_extensions, old_implicit_casting,
           motherduck_token, memory_limit, azure_transport_option_type, attach_mode], :as details}]
  (let [[database_file_base database_file_additional_options] (database-file-path-split database_file)]
    (-> details
        (merge
         {:classname         "org.duckdb.DuckDBDriver"
          :subprotocol       "duckdb"
          :subname           (or database_file "")
          "duckdb.read_only" (str read_only)
          "custom_user_agent" (str "metabase" (if (is-hosted?) " metabase-cloud" ""))
          "temp_directory"   (str database_file_base ".tmp")
          "jdbc_stream_results" "true"
          :TimeZone  "UTC"}
         (when old_implicit_casting
           {"old_implicit_casting" (str old_implicit_casting)})
         (when memory_limit
           {"memory_limit" (str memory_limit)})
         (when azure_transport_option_type
           {"azure_transport_option_type" (str azure_transport_option_type)})
         (when allow_unsigned_extensions
           {"allow_unsigned_extensions" (str allow_unsigned_extensions)})
         (when (seq (re-find #"^md:" database_file))
            ;; attach_mode option is not settable by the user, it's always single mode when
            ;; using motherduck, but in tests we need to be able to connect to motherduck in
            ;; workspace mode, so it's handled here.
           {"motherduck_attach_mode"  (or attach_mode "single")})    ;; when connecting to MotherDuck, explicitly connect to a single database
         (when (seq motherduck_token)     ;; Only configure the option if token is provided
           {"motherduck_token" motherduck_token})
         (sql-jdbc.common/additional-options->map (:additional-options details) :url)
         (sql-jdbc.common/additional-options->map database_file_additional_options :url))
        ;; remove fields from the metabase config that do not directly go into the jdbc spec
        (dissoc :database_file :read_only :port :engine :allow_unsigned_extensions
                :old_implicit_casting :motherduck_token :memory_limit :azure_transport_option_type
                :advanced-options :additional-options :attach_mode :init_sql))))

(defn- remove-keys-with-prefix [details prefix]
  (apply dissoc details (filter #(str/starts-with? (name %) prefix) (keys details))))

(defmethod sql-jdbc.conn/connection-details->spec :duckdb
  [_ details-map]
  (-> details-map
      (merge {:motherduck_token (get-motherduck-token details-map)})
      (remove-keys-with-prefix "motherduck_token-")
      jdbc-spec))

(defmethod sql-jdbc.conn/data-warehouse-connection-pool-properties :duckdb
  [driver database]
  (assoc ((get-method sql-jdbc.conn/data-warehouse-connection-pool-properties :default)
          driver
          database)
         "maxPoolSize" 1))

(defmethod sql-jdbc.execute/do-with-connection-with-options :duckdb
  [driver db-or-id-or-spec {:keys [^String session-timezone report-timezone] :as options} f]
  ;; First use the parent implementation to get the connection with standard options
  (sql-jdbc.execute/do-with-resolved-connection
   driver
   db-or-id-or-spec
   options
   (fn [^Connection conn]
     (when (not (sql-jdbc.execute/recursive-connection?))
       (when-let [init-sql (-> db-or-id-or-spec :details :init_sql)]
         (when (seq (str/trim init-sql))
           (let [db-key (get-database-key db-or-id-or-spec)
                 init-state (.computeIfAbsent init-sql-states db-key
                                            (fn [_] (AtomicBoolean. false)))]
             ;; Ensure init SQL is executed only once
             (when (.compareAndSet ^AtomicBoolean init-state false true)
               (.lock init-sql-lock)
               (log/infof "DuckDB init SQL has not been executed for this database, executing now...")
               (try
                 (with-open [stmt (.createStatement conn)]
                   (.execute stmt init-sql)
                   (log/tracef "Successfully executed DuckDB init SQL"))
                 (catch Throwable e
                   ;; If init SQL fails, reset the state so it can be retried
                   (.set ^AtomicBoolean init-state false)
                   (log/errorf e "Failed to execute DuckDB init SQL"))
                 (finally
                   (.unlock init-sql-lock))))))))
     ;; Additionally set timezone if provided and we're not in a recursive connection
     (when (and (or report-timezone session-timezone) (not (sql-jdbc.execute/recursive-connection?)))
       (let [timezone-to-use (or report-timezone session-timezone)]
         (try
           (with-open [stmt (.createStatement conn)]
             (.execute stmt (format "SET TimeZone='%s';" timezone-to-use)))
           (catch Throwable e
             (log/debugf e "Error setting timezone '%s' for DuckDB database" timezone-to-use)))))
     ;; Call the function with the configured connection
     (f conn))))

(defmethod sql-jdbc.execute/set-timezone-sql :duckdb [_]
  "SET GLOBAL TimeZone=%s;")

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [[#"BOOLEAN"                  :type/Boolean]
    [#"BOOL"                     :type/Boolean]
    [#"LOGICAL"                  :type/Boolean]
    [#"HUGEINT"                  :type/BigInteger]
    [#"UBIGINT"                  :type/BigInteger]
    [#"BIGINT"                   :type/BigInteger]
    [#"INT8"                     :type/BigInteger]
    [#"LONG"                     :type/BigInteger]
    [#"INT4"                     :type/Integer]
    [#"SIGNED"                   :type/Integer]
    [#"INT2"                     :type/Integer]
    [#"SHORT"                    :type/Integer]
    [#"INT1"                     :type/Integer]
    [#"UINTEGER"                 :type/Integer]
    [#"USMALLINT"                :type/Integer]
    [#"UTINYINT"                 :type/Integer]
    [#"INTEGER"                  :type/Integer]
    [#"SMALLINT"                 :type/Integer]
    [#"TINYINT"                  :type/Integer]
    [#"INT"                      :type/Integer]
    [#"DECIMAL"                  :type/Decimal]
    [#"DOUBLE"                   :type/Float]
    [#"FLOAT8"                   :type/Float]
    [#"NUMERIC"                  :type/Float]
    [#"REAL"                     :type/Float]
    [#"FLOAT4"                   :type/Float]
    [#"FLOAT"                    :type/Float]
    [#"VARCHAR"                  :type/Text]
    [#"BPCHAR"                   :type/Text]
    [#"CHAR"                     :type/Text]
    [#"TEXT"                     :type/Text]
    [#"STRING"                   :type/Text]
    [#"JSON"                     :type/JSON]
    [#"BLOB"                     :type/*]
    [#"BYTEA"                    :type/*]
    [#"VARBINARY"                :type/*]
    [#"BINARY"                   :type/*]
    [#"UUID"                     :type/UUID]
    [#"TIMESTAMPTZ"              :type/DateTimeWithTZ]
    [#"TIMESTAMP WITH TIME ZONE" :type/DateTimeWithTZ]
    [#"DATETIME"                 :type/DateTime]
    [#"TIMESTAMP_S"              :type/DateTime]
    [#"TIMESTAMP_MS"             :type/DateTime]
    [#"TIMESTAMP_NS"             :type/DateTime]
    [#"TIMESTAMP"                :type/DateTime]
    [#"DATE"                     :type/Date]
    [#"TIME"                     :type/Time]
    [#"GEOMETRY"                 :type/*]]))

(defmethod sql-jdbc.sync/database-type->base-type :duckdb
  [_ field-type]
  (database-type->base-type field-type))

(defn- local-time-to-time [^LocalTime lt]
  (Time. (.getLong lt ChronoField/MILLI_OF_DAY)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb LocalDate]
  [_ ^PreparedStatement prepared-statement i t]
  (.setObject prepared-statement i (t/local-date-time t (t/local-time 0))))

(defmethod sql-jdbc.execute/set-parameter [:duckdb LocalTime]
  [_ ^PreparedStatement prepared-statement i t]
  (.setObject prepared-statement i (local-time-to-time t)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb OffsetTime]
  [_ ^PreparedStatement prepared-statement i ^OffsetTime t]
  (let [adjusted-tz  (local-time-to-time (t/local-time (t/with-offset-same-instant t (t/zone-offset 0))))]
    (.setObject prepared-statement i adjusted-tz)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb String]
  [_ ^PreparedStatement prepared-statement i t]
  (.setObject prepared-statement i t))

;; .getObject of DuckDB (v0.4.0) does't handle the java.time.LocalDate but sql.Date only,
;; so get the sql.Date from DuckDB and convert it to java.time.LocalDate
(defmethod sql-jdbc.execute/read-column-thunk [:duckdb Types/DATE]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [sqlDate (.getDate rs i)]
      (.toLocalDate sqlDate))))

;; .getObject of DuckDB (v0.4.0) does't handle the java.time.LocalTime but sql.Time only,
;; so get the sql.Time from DuckDB and convert it to java.time.LocalTime
(defmethod sql-jdbc.execute/read-column-thunk [:duckdb Types/TIME]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [sql-time-string (.getString rs i)]
      (LocalTime/parse sql-time-string))))

;; override the sql-jdbc.execute/read-column-thunk for TIMESTAMP based on
;; DuckDB JDBC implementation.
(defmethod sql-jdbc.execute/read-column-thunk [:duckdb Types/TIMESTAMP]
  [_ ^ResultSet rs _ ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/local-date-time t))))

;; date processing for aggregation
(defmethod driver/db-start-of-week :duckdb [_] :monday)

(defmethod sql.qp/add-interval-honeysql-form :duckdb
  [driver hsql-form amount unit]
  (if (= unit :quarter)
    (recur driver hsql-form (* amount 3) :month)
    (h2x/+ (h2x/->timestamp-with-time-zone hsql-form) [:raw (format "(INTERVAL '%d' %s)" (int amount) (name unit))])))

(defmethod sql.qp/date [:duckdb :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:duckdb :minute]          [_ _ expr] [:date_trunc (h2x/literal :minute) expr])
(defmethod sql.qp/date [:duckdb :minute-of-hour]  [_ _ expr] [:minute expr])
(defmethod sql.qp/date [:duckdb :hour]            [_ _ expr] [:date_trunc (h2x/literal :hour) expr])
(defmethod sql.qp/date [:duckdb :hour-of-day]     [_ _ expr] [:hour expr])
(defmethod sql.qp/date [:duckdb :day]             [_ _ expr] [:date_trunc (h2x/literal :day) expr])
(defmethod sql.qp/date [:duckdb :day-of-month]    [_ _ expr] [:day expr])
(defmethod sql.qp/date [:duckdb :day-of-year]     [_ _ expr] [:dayofyear expr])

(defmethod sql.qp/date [:duckdb :day-of-week]
  [driver _ expr]
  (sql.qp/adjust-day-of-week driver [:isodow expr]))

(defmethod sql.qp/date [:duckdb :week]
  [driver _ expr]
  (sql.qp/adjust-start-of-week driver (partial conj [:date_trunc] (h2x/literal :week)) expr))

(defmethod sql.qp/date [:duckdb :month]           [_ _ expr] [:date_trunc (h2x/literal :month) expr])
(defmethod sql.qp/date [:duckdb :month-of-year]   [_ _ expr] [:month expr])
(defmethod sql.qp/date [:duckdb :quarter]         [_ _ expr] [:date_trunc (h2x/literal :quarter) expr])
(defmethod sql.qp/date [:duckdb :quarter-of-year] [_ _ expr] [:quarter expr])
(defmethod sql.qp/date [:duckdb :year]            [_ _ expr] [:date_trunc (h2x/literal :year) expr])

(defmethod sql.qp/datetime-diff [:duckdb :year]
  [_driver _unit x y]
  [:datesub (h2x/literal :year) (h2x/cast "date" x) (h2x/cast "date" y)])

(defmethod sql.qp/datetime-diff [:duckdb :quarter]
  [_driver _unit x y]
  [:datesub (h2x/literal :quarter) (h2x/cast "date" x) (h2x/cast "date" y)])

(defmethod sql.qp/datetime-diff [:duckdb :month]
  [_driver _unit x y]
  [:datesub (h2x/literal :month) (h2x/cast "date" x) (h2x/cast "date" y)])

(defmethod sql.qp/datetime-diff [:duckdb :week]
  [_driver _unit x y]
  (h2x// [:datesub (h2x/literal :day) (h2x/cast "date" x) (h2x/cast "date" y)] 7))

(defmethod sql.qp/datetime-diff [:duckdb :day]
  [_driver _unit x y]
  [:datesub (h2x/literal :day) (h2x/cast "date" x) (h2x/cast "date" y)])

(defmethod sql.qp/datetime-diff [:duckdb :hour]
  [_driver _unit x y]
  [:datesub (h2x/literal :hour) x y])

(defmethod sql.qp/datetime-diff [:duckdb :minute]
  [_driver _unit x y]
  [:datesub (h2x/literal :minute) x y])

(defmethod sql.qp/datetime-diff [:duckdb :second]
  [_driver _unit x y]
  [:datesub (h2x/literal :second) x y])

(defmethod sql.qp/unix-timestamp->honeysql [:duckdb :seconds]
  [_ _ expr]
  [:to_timestamp (h2x/cast :DOUBLE expr)])

(defmethod sql.qp/->honeysql [:duckdb :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)])

;; empty result set for queries without result (like insert...)
(defn- empty-rs []
  (reify
    ResultSet
    (getMetaData [_]
      (reify
        ResultSetMetaData
        (getColumnCount [_] 1)
        (getColumnLabel [_ _idx] "WARNING")
        (getColumnTypeName [_ _] "CHAR")
        (getColumnType [_ _] Types/CHAR)))
    (next [_] false)
    (close [_])))

;; override native execute-statement! to make queries that does't returns ResultSet

(defmethod sql-jdbc.execute/execute-statement! :duckdb
  [_driver ^Statement stmt ^String sql]
  (if (.execute stmt sql)
    (.getResultSet stmt)
    (empty-rs)))

(defn- is_motherduck
  [database_file]
  (and (seq (re-find #"^md:" database_file)) (> (count database_file) 3)))

(defn- motherduck_db_name
  [database_file]
  (subs database_file 3))

;; Creates a new connection to the same DuckDB instance to avoid deadlocks during concurrent operations.
;; context: observed in tests that sometimes multiple syncs can be triggered on the same db at the same time,
;; (and potentially the deletion of the local duckdb file) that results in bad_weak_ptr errors on the duckdb
;; connection object and deadlocks, so creating a lightweight clone of the connection to the same duckdb
;; instance to avoid deadlocks.
(defn- clone-raw-connection [connection]
  (let [c3p0-conn (cast com.mchange.v2.c3p0.C3P0ProxyConnection connection)
        clone-method (.getMethod org.duckdb.DuckDBConnection "duplicate" (into-array Class []))
        raw-conn-token com.mchange.v2.c3p0.C3P0ProxyConnection/RAW_CONNECTION
        args (into-array Object [])]
    (.rawConnectionOperation c3p0-conn clone-method raw-conn-token args)))

(defmethod driver/describe-database :duckdb
  [driver database]
  (let
   [database_file (get (get database :details) :database_file)
    database_file (first (database-file-path-split database_file))  ;; remove additional options in connection string
    get_tables_query (str "select * from information_schema.tables "
                               ;; Additionally filter by db_name if connecting to MotherDuck, since
                               ;; multiple databases can be attached and information about the
                               ;; non-target database will be present in information_schema.
                          (if (is_motherduck database_file)
                            (let [db_name_without_md (motherduck_db_name database_file)]
                              (format "where table_catalog = '%s' " db_name_without_md))
                            ""))]
    {:tables
     (sql-jdbc.execute/do-with-connection-with-options
      driver database nil
      (fn [conn]
        (set
         (for [{:keys [table_schema table_name]}
               (jdbc/query {:connection (clone-raw-connection conn)}
                           [get_tables_query])]
           {:name          table_name
            :schema        nil
            :duckdb/schema table_schema}))))}))

(defmethod driver/describe-table :duckdb
  [driver database {table_name :name, schema :schema, duckdb-schema :duckdb/schema}]
  (let [database_file (get (get database :details) :database_file)
        database_file (first (database-file-path-split database_file))  ;; remove additional options in connection string
        schema-name (or duckdb-schema schema)
        get_columns_query (str
                           (format
                            "select * from information_schema.columns where table_name = '%s'"
                            table_name)
                           (when schema-name
                             (format " and table_schema = '%s'" schema-name))
                                  ;; Additionally filter by db_name if connecting to MotherDuck, since
                                  ;; multiple databases can be attached and information about the
                                  ;; non-target database will be present in information_schema.
                           (if (is_motherduck database_file)
                             (let [db_name_without_md (motherduck_db_name database_file)]
                               (format "and table_catalog = '%s' " db_name_without_md))
                             ""))]
    {:name   table_name
     :schema nil
     :duckdb/schema schema-name
     :fields
     (sql-jdbc.execute/do-with-connection-with-options
      driver database nil
      (fn [conn] (let [results (jdbc/query
                                {:connection (clone-raw-connection conn)}
                                [get_columns_query])]
                   (set
                    (for [[idx {column_name :column_name, data_type :data_type}] (m/indexed results)]
                      {:name              column_name
                       :database-type     data_type
                       :base-type         (sql-jdbc.sync/database-type->base-type driver (keyword data_type))
                       :database-position idx})))))}))

;; The 0.4.0 DuckDB JDBC .getImportedKeys method throws 'not implemented' yet.
;; There is no support of FK yet.
(defmethod driver/describe-table-fks :duckdb
  [_ _ _]
  nil)

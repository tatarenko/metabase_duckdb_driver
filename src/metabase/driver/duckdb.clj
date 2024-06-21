(ns metabase.driver.duckdb
  (:require [clojure.java.jdbc :as jdbc]
            [java-time.api :as t]
            [medley.core :as m]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.models.secret :as secret]
            [metabase.public-settings.premium-features :as premium-features]
            [metabase.util.honey-sql-2 :as h2x])
  (:import [java.sql
            ResultSet
            ResultSetMetaData
            Statement
            Time
            Types]
           [java.time LocalDate LocalTime OffsetTime]
           [java.time.temporal ChronoField]
           ))

(driver/register! :duckdb, :parent :sql-jdbc)

(defn- jdbc-spec
  "Creates a spec for `clojure.java.jdbc` to use for connecting to DuckDB via JDBC from the given `opts`"
  [{:keys [database_file, read_only, allow_unsigned_extensions, old_implicit_casting, motherduck_token], :as details}] 
  (-> details 
      (merge
       {:classname         "org.duckdb.DuckDBDriver"
        :subprotocol       "duckdb"
        :subname           (or database_file "")
        "duckdb.read_only" (str read_only) 
        "allow_unsigned_extensions" (str allow_unsigned_extensions)
        "custom_user_agent" (str "metabase" (if premium-features/is-hosted? " metabase-cloud" ""))
        "temp_directory"   (str database_file ".tmp")
        "old_implicit_casting" (str old_implicit_casting)
        "jdbc_stream_results" "true"}
       (when (seq (re-find #"^md:" database_file)) 
         {"motherduck_attach_mode"  "single"})    ;; when connecting to MotherDuck, explicitly connect to a single database
       (when (seq motherduck_token)     ;; Only configure the option if token is provided
         {"motherduck_token" motherduck_token}))
      (dissoc details :database_file :read_only :allow_unsigned_extensions :port :engine :motherduck_token :motherduck_token-value)
      sql-jdbc.common/handle-additional-options))

(defmethod sql-jdbc.conn/connection-details->spec :duckdb
  [_ details-map]
  (-> details-map 
      (merge {:motherduck_token (or (-> (secret/db-details-prop->secret-map details-map "motherduck_token") 
                                        secret/value->string) 
                                    (secret/get-secret-string details-map "motherduck_token"))})
      jdbc-spec))

(defmethod sql-jdbc.execute/set-timezone-sql :duckdb [_]
  "SET GLOBAL TimeZone=%s;")

(def ^:private database-type->base-type 
  (sql-jdbc.sync/pattern-based-database-type->base-type 
   [[#"BOOLEAN"      :type/Boolean]
    [#"BOOL"         :type/Boolean]
    [#"LOGICAL"      :type/Boolean]
    [#"HUGEINT"      :type/BigInteger]
    [#"BIGINT"       :type/BigInteger]
    [#"UBIGINT"      :type/BigInteger]  ; ineffective
    [#"INT8"         :type/BigInteger]
    [#"LONG"         :type/BigInteger]
    [#"INT"          :type/Integer]
    [#"INTEGER"      :type/Integer]     ; ineffective
    [#"INT4"         :type/Integer]     ; ineffective
    [#"SIGNED"       :type/Integer]
    [#"SMALLINT"     :type/Integer]     ; ineffective
    [#"INT2"         :type/Integer]     ; ineffective
    [#"SHORT"        :type/Integer]
    [#"TINYINT"      :type/Integer]     ; ineffective
    [#"INT1"         :type/Integer]     ; ineffective
    [#"UINTEGER"     :type/Integer]     ; ineffective
    [#"USMALLINT"    :type/Integer]     ; ineffective
    [#"UTINYINT"     :type/Integer]     ; ineffective
    [#"DECIMAL"      :type/Decimal]
    [#"DOUBLE"       :type/Float]
    [#"FLOAT8"       :type/Float]
    [#"NUMERIC"      :type/Float]
    [#"REAL"         :type/Float]
    [#"FLOAT4"       :type/Float]
    [#"FLOAT"        :type/Float]
    [#"VARCHAR"      :type/Text]
    [#"CHAR"         :type/Text]
    [#"BPCHAR"       :type/Text]        ; ineffective
    [#"TEXT"         :type/Text]
    [#"STRING"       :type/Text]
    [#"BLOB"         :type/*]
    [#"BYTEA"        :type/*]
    [#"BINARY"       :type/*]
    [#"VARBINARY"    :type/*]           ; ineffective
    [#"UUID"         :type/UUID]
    [#"TIMESTAMPTZ"  :type/DateTimeWithTZ]
    [#"TIMESTAMP"    :type/DateTime]
    [#"DATETIME"     :type/DateTime]
    [#"TIMESTAMP_S"  :type/DateTime]               ; ineffective
    [#"TIMESTAMP_MS" :type/DateTime]               ; ineffective
    [#"TIMESTAMP_NS" :type/DateTime]               ; ineffective
    [#"DATE"         :type/Date]
    [#"TIME"         :type/Time]]))

(defmethod sql-jdbc.sync/database-type->base-type :duckdb
  [_ field-type]
  (database-type->base-type field-type))


(defn- local-time-to-time [lt]
  (Time. (.getLong lt ChronoField/MILLI_OF_DAY)))

(defn- time-to-local-time [t]
  (let [date-time (.getTime t)]
    (LocalTime/ofNanoOfDay (* 1000000 date-time))))

(defmethod sql-jdbc.execute/set-parameter [:duckdb LocalDate]
  [_ prepared-statement i t] 
  (.setObject prepared-statement i (t/local-date-time t (t/local-time 0))))

(defmethod sql-jdbc.execute/set-parameter [:duckdb LocalTime]
  [_ prepared-statement i t]
  (.setObject prepared-statement i (local-time-to-time t)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb OffsetTime]
  [_ prepared-statement i t] 
  (let [adjusted-tz  (local-time-to-time (.toLocalTime (t/with-offset-same-instant t (t/zone-offset 0))))] 
    (.setObject prepared-statement i adjusted-tz)))

(defmethod sql-jdbc.execute/set-parameter [:duckdb String]
  [_ prepared-statement i t] 
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
    (when-let [sqlTimeStamp (.getTime rs i)]
      (time-to-local-time sqlTimeStamp))))

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
    (h2x/+ (h2x/->timestamp hsql-form) [:raw (format "(INTERVAL '%d' %s)" (int amount) (name unit))])))

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

(defmethod sql.qp/unix-timestamp->honeysql [:duckdb :seconds]
  [_ _ expr]
  [:make_timestamp (h2x/cast :DOUBLE expr)])

(defmethod sql.qp/->honeysql [:duckdb :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) 0])

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

(defmethod driver/describe-database :duckdb
  [driver database]
  (let [get_tables_query "select * from information_schema.tables"]
    {:tables
     (sql-jdbc.execute/do-with-connection-with-options
      driver database nil
      (fn [conn]
        (set
         (for [{:keys [table_schema table_name]}
               (jdbc/query {:connection conn}
                           [get_tables_query])]
           {:name table_name :schema table_schema}))))}))



(defmethod driver/describe-table :duckdb
  [driver database {table_name :name, schema :schema}]
  (let [get_columns_query (format 
                           "select * from information_schema.columns where table_name = '%s' and table_schema = '%s'" 
                           table_name schema)] 
    {:name   table_name
     :schema schema
     :fields
     (sql-jdbc.execute/do-with-connection-with-options
      driver database nil
      (fn [conn] (let [results (jdbc/query
                                {:connection conn}
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
  (set #{}))

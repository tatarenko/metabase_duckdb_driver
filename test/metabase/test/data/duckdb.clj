;; (require '[metabase.test.data.interface :as tx]) ; tx = test extensions
;; (require '[metabase.test.data.sql :as sql.tx])   ; sql test extensions
(ns metabase.test.data.duckdb
  (:require 
  [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
  [metabase.test.data.sql :as sql.tx]))

;; (sql/add-test-extensions! :duckdb)

(set! *warn-on-reflection* true)

(sql-jdbc.tx/add-test-extensions! :duckdb)

;; (defmethod sql.tx/pk-sql-type :duckdb [_] "INTEGER NOT NULL")

(ns metabase.test.data.duckdb
  (:require 
  [metabase.test.data.sql-jdbc :as sql-jdbc.tx]))

(set! *warn-on-reflection* true)

(sql-jdbc.tx/add-test-extensions! :duckdb)
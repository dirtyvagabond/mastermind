(ns mastermind.cwm
  (:import [java.io BufferedReader FileReader])
  (:require [clojure.string :as str]
            [clojure-csv.core :as csv]
            [clojure.java.io :as io]))


(def RESULTS-FILE "Batch_981269_batch_results.cwm_urls.csv")

;;
;; Results Processing
;;

(defn make-row
  "Given headers and a record, returns a hash-map where keys are
   the column names from headers and values are the corresponding
   values from the record."
  [header record]
  (into {} (map #(vector %1 %2) header record)))

(defn get-raw-results [results-file]
  (let [recs (csv/parse-csv (io/reader results-file))
        header (first recs)]
    (map #(make-row header %) (rest recs))))

(defn make-result
  "Returns structured question and answer pairs from a raw result.
   Makes a lot of hard-coded assumptions about the structure and naming
   of question and answer keys, values, etc."
  [raw-result]
  ;;keys for a question are like 'Input.[TOK]_[NDX]'
  ;;keys for the answers to a question are like 'Answer.[TOK]_[NDX]'
  (merge
     (into {}
           (map (fn [[k v]]
                   [(keyword k) v])
                raw-result))))

(defn get-results
  "Returns a seq of results read from results-file."
  [results-file]
  (map make-result (get-raw-results results-file)))

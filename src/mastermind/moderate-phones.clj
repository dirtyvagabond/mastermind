(ns mastermind.moderate-phones
  (:import [java.io BufferedReader FileReader]
           [java.net URLEncoder])
  (:require [clojure.string :as str]
            [clojure-csv.core :as csv]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [factql.core :as facts]))


;;(def CC-FILE "moderate-phones.csv")

;;(def RESULTS-FILE "Batch_972607_batch_results.cc.csv")


;;
;; Questions and HITs
;;

;; We will need to query Factual for head data when forming questions
(facts/init!)

;; Master map of Factual's country codes => Human readable country name
(defn get-country-codes []
  (let [lines (str/split (slurp "country-codes.csv") #"\n")]
    (reduce
     (fn [accum line]
       (let [[name code] (str/split line #",")]
         (if code
           (assoc accum (str/lower-case code) name)
           accum)))
     {}
     lines)))

(defn get-head-data [factual-id]
  (let [country-codes (get-country-codes)
        head (select-keys
              (first (facts/select places (where (= :factual_id factual-id))))
              [:country :name :locality :address])]
    (update-in head [:country] #(get country-codes (str/lower-case %)))))

(defn q-from-mod [rec]
  (let [;;old-tel (get-in rec ["meta" "rawPayload" "vendor" "originalPlace" "business" "telephone"])
        factual-id (get rec "factual_id")
        head (get-head-data factual-id)
        ;;name (get-in rec ["meta" "rawPayload" "vendor" "originalPlace" "business" "name"])
        ;;addr (get-in rec ["meta" "rawPayload" "vendor" "originalPlace" "address" "formattedAddressLines"])
        corr (get-in rec ["meta" "rawPayload" "reviewer" "dataProblem" "correctedField"])
        new-tel (get (first corr) "correctedValue")]
    (merge
     head
     {:id (rec "_id")
      :factual-id factual-id
;;      :name name
;;      :addr (str/join ", " addr)
      :new-tel new-tel})))

(defn whole? [question]
  (and
    (get question :name)
    (get question :address)
    (get question :locality)
    (get question :country)))

(defn get-questions
  "Returns the set of unique questions read from json-file. Each question will be a hash-map
   with all raw question attributes, plus the attributes used by our MTurk template:
     :id
     :tel"
  [json-file]
  (let [recs (json/parse-string (slurp json-file))
        recs (take 100 recs)
        ]
    (filter whole?
            (map q-from-mod recs))))

(defn hits-csv
  "Returns the full CSV content for an MTurk hits file."
  [questions]
  (str "question-id,factual-id,name,addr,locality,country,new-tel\n"
       (csv/write-csv (map
                       (fn [{:keys [id factual-id name address locality country new-tel]}]
                         [id factual-id name address locality country new-tel])
                       questions))))

(defn create-hits-file [hits-file mod-file]
  (spit hits-file (hits-csv (get-questions mod-file))))

;; TODO:
;;   Some automation to filter out obvious mistakes, e.g. a 2 digit phone number

;; TODO:
;;   don't include old telephone
;;   DO include all basic entity data; name, address, etc.
;;
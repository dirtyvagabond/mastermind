(ns mastermind.cc
  (:import [java.io BufferedReader FileReader]
           [java.net URLEncoder])
  (:require [clojure.string :as str]
            [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [factql.core :as facts]))


(def CC-FILE "credit-card-x.csv")

(def RESULTS-FILE "Batch_972607_batch_results.cc.csv")

;;
;; Questions and HITs
;;

(defn get-hash [type data]
  (.digest (java.security.MessageDigest/getInstance type) (.getBytes data) ))

(defn sha1-hash [data]
  (get-hash "sha1" data))

(defn get-hash-str [data-bytes]
  (apply str
  (map
    #(.substring
      (Integer/toString
    (+ (bit-and % 0xff) 0x100) 16) 1)
    data-bytes)))

(defn sha1-id [{:keys [desc suburb state zip]}]
  (get-hash-str (sha1-hash (format "%s %s %s %s" desc suburb state (str zip)))))

(defn assoc-id [m]
  (assoc m :id (sha1-id m)))

(defn rec-map [r]
  (->
   {:desc (first r)
    :street (second r)
    :suburb (nth r 2)
    :state (nth r 3)
    :zip (nth r 4)}
   assoc-id))

(defn query-text [{:keys [desc suburb state zip]}]
  (format "%s %s %s %s" desc suburb state (str zip)))

;;http://www.google.com/search?q=DUNKIN%09%23331807,%09,%09HICKSVILLE,%09NY,%0911801
(defn query-url [m]
  (str
     "http://www.google.com/search?q="
     (URLEncoder/encode (query-text m))))

(defn get-questions
  "Returns the set of unique questions read from csv-file. Each question will be a hash-map
   with all raw question attributes, plus the attributes used by our MTurk template:
     :id
     :query-url
     :query-text"
  [csv-file]
  (with-open [rdr (BufferedReader. (FileReader. csv-file))]
    (let [qs (rest (csv/parse-csv rdr))
          qs (into #{} (doall (map rec-map qs)))]
      (map
       (fn [q]
         (merge
          q
          {:query-url (query-url q)
           :query-text (query-text q)}))
       qs))))

(defn hits-csv
  "Returns the full CSV content for an MTurk hits file."
  [questions]
  (str "question-id,query-url,query-text\n"
       (csv/write-csv (map
                       (fn [{:keys [id query-url query-text]}]
                         [id query-url query-text])
                       questions))))
(defn create-hits-file [hits-file questions]
  (spit hits-file (hits-csv questions)))

;;
;; Resolving
;;

(defn values-for-resolve [q]
  {:name (:desc q)
   :address (:street q)
   :neighborhood (:suburb q)
   :country "us"
   :region (:state q)
   :postcode (:zip q)})

(defn resolve-answer
  "Given question q and answer a, returns the result of running resolve using
   the initial data from the hit plus using the answer's values for name and
   addresss."
  [q a]
  (facts/resolve-vals
   (merge
    (values-for-resolve q)
    {:name (:Answer.biz-name a)
     :address (:Answer.biz-address a)})))

(defn add-resolves-from-answers
  "Assumes question q already contains an entry for :answers"
  [q]
  (assoc q :answers
         (map #(assoc % :resolved (resolve-answer q %)) (:answers q))))

(defn resolve-question [q]
  (assoc q :resolved
         (-> q
             values-for-resolve
             facts/resolve-vals)))

(defn resolve-questions [qs]
  (map resolve-question qs))

(defn only-unresolved [questions]
  (filter
    #(empty? (:resolved %)) questions))

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

(defn merge-answers
  "Return question, with all available corresponding answers associated as :answers."
  [question all-answers]
  (let [answers (filter #(= (:Input.question-id %) (:id question)) all-answers)]
    (assoc question :answers answers)))

(defn get-questions-with-answers
  "Reads original questions from in-file and merges in results from results-file."
  [in-file results-file]
  (let [questions (get-questions in-file)
        all-answers (get-results results-file)]
    (map #(merge-answers % all-answers) questions)))

(defn get-all [in-file results-file]
  (resolve-questions
   (map add-resolves-from-answers
        (get-questions-with-answers in-file results-file))))

(defn filter-no-easy-resolve [results]
  (filter #(empty? (:resolved %)) results))

(defn add-resolve-from-answers-summary [result]
  (let [answers (:answers result)
        summary (map #(not (empty? (:resolved %))) answers)]
    (assoc result :resolve-from-answers-summary summary)))

(defn has-at-least-one-resolve-from-answers? [result]
  (let [answers (:answers result)]
    (some #(not (empty? (:resolved %))) answers)))

(defn needed-multi-answers? [result]
  (let [answers (:answers result)]
    (and
     (empty? (:resolved (first answers)))
     (or (not (empty? (:resolved (second answers))))
         (not (empty? (:resolved (nth answers 2))))))))

(defn filter-answers-successes
  "Returns the results that have at least one successful resolve by using the
   answer data."
  [results]
  (filter
   (fn [result]
     (let [answers (:answers result)
           resolves (apply concat (map :resolved answers))]
       (> (count resolves) 0)
       ;;(not (empty? (:resolve (first answers))))
       ))
   results))
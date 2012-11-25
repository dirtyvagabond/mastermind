(ns mastermind.cc
  (:import [java.io BufferedReader FileReader]
           [java.net URLEncoder])
  (:require [clojure.string :as str]
            [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [factql.core :as facts]))


(def CC-FILE "credit-card-x.csv")

(def RESULTS-FILE "Batch_971954_batch_results.cc.csv")

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

(defn get-hit-recs [csv-file]
  (with-open [rdr (BufferedReader. (FileReader. csv-file))]
    (let [recs (rest (csv/parse-csv rdr))]
      (into #{} (doall (map rec-map recs))))))

(defn hit-recs [rec-maps]
  (map-indexed (fn [i m]
                 ;; question-id, query-url, query-text
                 [(str i)
                  (query-url m)
                  (query-text m)])
               rec-maps))

(defn create-hits-csv-file [in out]
  (with-open [rdr (BufferedReader. (FileReader. in))]
    (let [recs (rest (csv/parse-csv rdr))
          rec-maps (map rec-map recs)]
      (spit out "question-id,query-url,query-text\n")
      (spit out (csv/write-csv (hit-recs rec-maps)) :append true))))

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

(defn resolve-query-from-hit [h]
  {:name (:desc h)
   :address (:street h)
   :neighborhood (:suburb h)
   :country "us"
   :region (:state h)
   :postcode (:zip h)})

(defn resolve-answer
  "Given hit h and answer a, returns the result of running resolve using
   the initial data from the hit plus using the answer's values for name and
   addresss."
  [h a]
  (facts/resolve-vals
   (merge
    (resolve-query-from-hit h)
    {:name (:Answer.name a)
     :address (:Answer.address a)})))

(defn resolve-hit [h]
  (facts/resolve-vals
   (resolve-query-from-hit h)))

(defn qndx<->qid [csv-file]
  (with-open [rdr (BufferedReader. (FileReader. csv-file))]
    (let [recs (rest (csv/parse-csv rdr))]
      (reduce
       (fn [acc rec]
         (-> acc
             (assoc (:id rec) (:index rec))
             (assoc (:index rec) (:id rec))))
       {}
       (map-indexed
        (fn [i rec]
          (assoc (rec-map rec) :index i))
        recs)))))

(defn merge-results
  "Return hit, with all available results merged in."
  [qndx results hit]
  (let [qndx (qndx (:id hit))
        results (filter #(= (:Input.question-id %) (str qndx)) results)]
    (-> hit
        (assoc :qndx qndx)
        (assoc :results results))))

(defn get-merged-results [in-file results-file]
  (let [results (get-results results-file)
        hits (get-hit-recs CC-FILE)
        qndx (qndx<->qid CC-FILE)
        results (map #(merge-results qndx results %) hits)]
    ;; filter out missing results (means we probably didn't upload the corresp. hits
    (filter #(> (count (:results %)) 0) results)))

(defn add-resolve-from-hit-data
  [hit]
  (assoc hit :raw-resolve (resolve-hit hit)))

(defn add-resolves-from-results
  "Assumes hit h already contains an entry for :results"
  [h]
  (assoc h :results
         (map #(assoc % :resolve (resolve-answer h %)) (:results h))))

(defn get-all [in-file results-file]
  (map add-resolve-from-hit-data
       (map add-resolves-from-results
            (get-merged-results in-file results-file))))

(defn filter-answers-successes [results]
  (filter
   (fn [result]
     (let [answers (:results result)
           resolves (apply concat  (map :resolve answers))]
       ;; (> (count resolves) 0)
       (not (empty? (:resolve (first answers))))

       ))
   results))
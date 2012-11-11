(ns mastermind.core
  (:require [clojure-csv.core :as csv]))

(defn get-lines []
  (clojure.string/split (slurp "underfold-queries.csv")
                        #"\n"))

(defn get-query-chunks []
  (partition 7 (get-lines)))

(defn into-record [a]
  {:answer  (get a 0)
   :id      (get a 1)
   :name    (get a 2)
   :address (get a 3)
   :city    (get a 4)
   :state   (get a 5)
   :zip     (get a 6)
   :phone   (get a 7)
   :lat     (get a 8)
   :lon     (get a 9)})

(defn parse-line [line]
  (first (csv/parse-csv line)))

(defn into-query [query-chunk]
  {:master     (into-record (parse-line (first query-chunk)))
   :candidates (map #(into-record (parse-line %)) (take 3 (drop 2 query-chunk)))})

(defn get-queries []
  (map into-query (get-query-chunks)))

(def HEADER "id_master,name_master,address_master,city_master,state_master,zip_master,phone_master,lat_master,lon_master,id_0,name_0,address_0,city_0,state_0,zip_0,phone_0,lat_0,lon_0,id_0,name_1,address_1,city_1,state_1,zip_1,phone_1,lat_1,lon_1,id_0,name_2,address_2,city_2,state_2,zip_2,phone_2,lat_2,lon_2\n")

(defn record-as-csv [rec]
  (csv/write-csv
   [(map #(or ( get rec %) "") [:id :name :address :city :state :zip :phone :lat :lon])]
   :end-of-line ""))

(defn hit-rec [query]
  (cons (:master query) (:candidates query)))

(defn hit-line-csv [query]
  (str
   (clojure.string/join ","
                        (map record-as-csv (hit-rec query)))
   "\n"))

(defn write-hit-file [queries]
  (spit "hits.csv"
        (apply str (cons HEADER (map hit-line-csv queries)))))

(def results-file "Batch_958390_batch_results.csv")

(defn make-row [header rec]
  (into {} (map #(vector %1 %2) header rec)))

(defn get-results []
  (let [recs (csv/parse-csv (clojure.java.io/reader results-file))
        header (first recs)]
    (map #(make-row header %) (rest recs))))

(defn expected [master-id ndx queries]
  (let [query (first (filter #(= (get-in % [:master :id]) master-id) queries))
        candidate (get (into [] (:candidates query)) ndx)]
    (:answer candidate)))

(defn translate-turker-answer [s]
  (condp = s
    "same place" "1"
    "different place" "0"
    "not sure" "?"))

(def scores (atom {:right 0
                   :wrong 0
                   :gimme 0
                   :confused 0
                   :total 0}))

(defn score-adj [expected result]
  (cond
    (= expected result "0") :right
    (= expected result "1") :right
    (= expected "?") :gimme
    (= result "?") :confused
    :else :wrong))

(defn update-scores [expected result]
  (let [adj (score-adj expected result)]
    (println "expected:" expected ", result:" result ", adj:" adj)
   (swap! scores #(update-in % [:total] inc))
    (swap! scores #(update-in % [adj] inc))))

(defn check-result [queries res master-id ndx]
  (let [expct (expected master-id ndx queries)
        answ  (translate-turker-answer (get res (str "Answer.m" ndx)))]
    (when (not (= expct "?"))
      (update-scores expct answ))))

(defn check-results []
  (let [queries (get-queries)
        results (get-results)]
    (doseq [res results]
      (let [master-id (get res "Input.id_master")]
        (println "result: --------------")
        (println "master-id:" master-id)
        (println "worker_id:" (get res "WorkerId"))
        (doseq [ndx (range 3)]
          (check-result queries res master-id ndx))))
    (println @scores)))

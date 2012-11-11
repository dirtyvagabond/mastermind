(ns mastermind.questions
  (:require [clojure.string :as str]
            [clojure-csv.core :as csv]))

(def HITS-FILE "underfold-queries.csv")

(defn parse-line [line]
  (first (csv/parse-csv line)))

(defn get-lines [csv-file]
  (str/split (slurp csv-file) #"\n"))

(defn get-hit-chunks [question-file]
  ;; current question file has 7 lines per question chunk
  (partition 7 (get-lines question-file)))

(defn groom-expected-answer [a]
  (condp = a
    "1" :yes
    "0" :no
    "?" :maybe))

(defn into-question [a]
  {:expected-answer  (groom-expected-answer (get a 0))
   :id               (get a 1)  ;; internal id for the hit, e.g. an entity id (this is NOT an MTurk HIT id)
   :name             (get a 2)
   :address          (get a 3)
   :city             (get a 4)
   :state            (get a 5)
   :zip              (get a 6)
   :phone            (get a 7)
   :lat              (get a 8)
   :lon              (get a 9)})

(defn into-hit
  "Makes a lot of hard-coded assumptions about format of the hits file"
  [hit-chunk]
    ;; id is an internal id for the hit, e.g. an entity id (this is NOT an MTurk HIT id)
    {:id (get (parse-line (first hit-chunk)) 1)
     :questions (map #(into-question (parse-line %)) (take 3 (drop 2 hit-chunk)))})

(defn get-hits [hits-file]
  (reduce
   (fn [acc hit]
     (assoc acc (:id hit) hit))
   {}
   (map into-hit (get-hit-chunks hits-file))))

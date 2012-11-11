(ns mastermind.results
  (:require [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [mastermind.questions :as q]))

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

(defn groom-answer [a]
  (condp = a
    "match" :yes
    "not a match" :no
    "not sure" :maybe))

(defn make-result
  "Returns structured question and answer pairs from a raw result.
   Makes a lot of hard-coded assumptions about the structure and naming
   of question and answer keys, values, etc."
  [raw-result]
  (def ANSW-TOKS ["record" "notes"])
  ;;keys for a question are like 'Input.[TOK]_[NDX]'
  ;;keys for the answers to a question are like 'Answer.[TOK]_[NDX]'
  (let [assignment-id  (raw-result "AssignmentId")
        question-id    (raw-result "Input.id_master")
        worker-id      (raw-result "WorkerId")
        work-time-secs (raw-result "WorkTimeInSeconds")
        inputs  (into {} (filter #(.startsWith (key %) "Input.") raw-result))
        answers (into {} (filter #(.startsWith (key %) "Answer.") raw-result))
        answers (map
                 (fn [ndx]
                   {:notes (answers (str "Answer.notes_" ndx))
                    :answer (groom-answer (answers (str "Answer.record_" ndx)))})
                 (range 3))]
    {:assignment-id assignment-id
     :question-id question-id
     :worker-id worker-id
     :work-time-secs work-time-secs
     :inputs inputs
     :answers answers}))

(defn get-results
  "Returns a hash-map of results read from results-file, keyed by assignment-id."
  [results-file]
  (reduce
   (fn [acc res]
     (assoc acc (:assignment-id res) res))
   {}
   (map make-result (get-raw-results results-file))))

(defn add-expected-answers
  "Returns result with its answers updated to include the associated
   :expected-answer from hit."
  [result hit]
  (let [questions (into [] (:questions hit))
        answers   (into [] (:answers result))]
    (assoc result :answers
           (map-indexed
            (fn [idx answer]
              (assoc answer
                :expected-answer (:expected-answer (get questions idx))))
            answers))))

(defn with-expected-answers
  "Returns a sequence of results, with added expected answers to all answers within results,
   based on hits."
  [results hits]
  (map
   (fn [result]
     (let [question-id (:question-id result)]
       (add-expected-answers result (get hits question-id))))
   (vals results)))

(defn score-adj [{:keys [expected-answer answer]}]
  (cond
    (= expected-answer answer) :right
    (= expected-answer :maybe) :gimme
    (= answer :maybe) :confused
    :else :wrong))

(defn score [results]
  (let [answers (apply concat (map :answers results))]
    (reduce
     (fn [scores ans]
       (update-in scores [(score-adj ans)] inc)
       )
     {:right 0
      :wrong 0
      :confused 0
      :gimme 0}
     answers)))

(defn score-job [hits-file results-file]
  (let [hits (q/get-hits hits-file)
        results (with-expected-answers (get-results results-file) hits)]
    (score results)))

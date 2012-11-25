(ns mastermind.results
  (:require [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [mastermind.questions :as q]))

(def ASSIGNMENT-SAMPLE-LIMIT 5)
(def CONSENSUS-MIN 3)

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
    "not sure" :maybe
    "different place" :no
    "same place" :yes
    nil nil))

(defn parse-answers [raw-result]
  (let [raw-answers (into {} (filter #(.startsWith (key %) "Answer.") raw-result))]
    (map
     (fn [ndx]
       (let [ra (or (raw-answers (str "Answer.record_" ndx))
                    (raw-answers (str "Answer.m" ndx)))]
         {:notes (raw-answers (str "Answer.notes_" ndx))
          :answer (groom-answer ra)}))
     (range 3))))

(defn make-result
  "Returns structured question and answer pairs from a raw result.
   Makes a lot of hard-coded assumptions about the structure and naming
   of question and answer keys, values, etc."
  [raw-result]
  ;;keys for a question are like 'Input.[TOK]_[NDX]'
  ;;keys for the answers to a question are like 'Answer.[TOK]_[NDX]'
  (merge
     {:answers (parse-answers raw-result)}
     (into {}
           (map (fn [[k v]]
                   [(keyword k) v])
                raw-result))))

(defn get-results
  "Returns a seq of results read from results-file."
  [results-file]
  (map make-result (get-raw-results results-file)))

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

(defn question-id [result]
  (:Input.id_master result))

(defn with-expected-answers
  "Returns a sequence of results, with added expected answers to all answers within results,
   based on hits."
  [results hits]
  (map
   (fn [result]
     (add-expected-answers result (get hits (question-id result))))
   results))

(defn score-adj [{:keys [expected-answer answer]}]
  (cond
    (= expected-answer answer) :right
    (= expected-answer :maybe) :gimme
    (= answer :maybe) :confused
    :else :wrong))

(defn score-answers [answers]
  (reduce
     (fn [scores ans]
       (update-in scores [(score-adj ans)] inc))
     {:right 0
      :wrong 0
      :confused 0
      :gimme 0}
     answers))

(defn score [results]
  (let [answers (apply concat (map :answers results))]
    (score-answers answers)))

(defn score-job [hits-file results-file]
  (let [hits (q/get-hits hits-file)
        results (with-expected-answers (get-results results-file) hits)]
    (score results)))

(defn question-id->results
  [results]
  (reduce
   (fn [acc result]
     (update-in acc [(question-id result)]
                #(conj % result)))
   {}
   results))

(defn consensus-from-answers [answers]
  (let [freqs (frequencies (map :answer answers))
        winner (ffirst (filter #(>= (val %) CONSENSUS-MIN) freqs))]
    {:expected-answer (:expected-answer (first answers))
     :answers answers
     :answer winner}))

(defn consensus-from-results
  [results]
  (reduce
   (fn [acc idx]
     (let [answers (map #(nth (get % :answers) idx) results)]
       (conj acc (consensus-from-answers answers))))
   []
   (range 3)))

(defn gather-consensus [results hits]
  (let [qid->results (question-id->results results)]
    ;; For each question, pick 3 results and try to find a
    ;; consensus. Where there's agreement, score based on
    ;; the correct answer.
    (reduce
     (fn [acc [qid results]]
       (assoc acc qid
              (consensus-from-results (take ASSIGNMENT-SAMPLE-LIMIT results))))
     {}
     qid->results)))

(defn score-answers
  "Returns an overall score result for all answers.
   answers must be a sequence of answer hash-map.
   Each answer must have an :answer and an :expected-answer."
  [answers]
  (reduce
   (fn [scores ans]
     (update-in scores [(score-adj ans)] inc))
   {:right 0
    :wrong 0
    :confused 0
    :gimme 0}
   answers))

(defn answers-accuracy [answers]
  (let [{:keys [right wrong]} (score-answers answers)
        total (+ right wrong)]
    {:accuracy (if (> total 0)
                 (float (/ right total))
                 -1)
     :total total}))

(defn assess-by-consensus [results-file hits-file]
  (let [results (get-results results-file)
        hits    (q/get-hits hits-file)
        results (with-expected-answers results hits)
        _ (println "Results count:" (count results))
        consensi (gather-consensus results hits)
        _ (println "Consensi count:" (count consensi))
        answers  (reduce
                   (fn [acc centry]
                     (apply conj acc centry))
                   []
                   (vals consensi))
        _ (println "Raw answers:" (count answers))
        answers (filter :answer answers)
        ]
    (score-for-all answers)))

(def BATCHES [959635 960038 959749 960404 958390])

(defn get-all-results []
  (apply concat (map
                 #(get-results (format "Batch_%s_batch_results.csv" %))
                 BATCHES)))

(defn unique-workers [results]
  (into #{} (map :WorkerId results)))

(defn results-from-worker [results wid]
  (filter #(= (% "WorkerId") wid) results))

(defn answers-with-result-meta [result]
  (map #(with-meta % (dissoc result :answers)) (:answers result)))

(defn answers-with-results-meta [results]
  (apply concat (map answers-with-result-meta results)))

(defn go []
  (let [hits (q/get-hits "underfold-queries.csv")
        results (with-expected-answers (get-all-results) hits)]
    (doseq [wid (unique-workers results)]
      (let [answers (answers-with-results-meta (filter #(= (:WorkerId %) wid) results))]
        (println wid (answers-accuracy answers))))))

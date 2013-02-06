(ns mastermind.db
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [digest :as digest]
            [clojure-csv.core :as csv]
            [cheshire.core :as json]))

(defn init! []
  (mg/connect!)
  (mg/set-db! (mg/get-db "monger-test")))

(defn normalize-keys [m]
  (into {}
        (for [[k v] m]
          [(name k) v])))
(defn normalize [col] (map name col))

;;TODO: need to do this intelligently, for many tasks across many projects, etc.
(defn make-id
  "Creates a consistent task id based on the notable keys of rec"
  ([rec ks]
     (let [rec (normalize-keys rec)
           s (apply str (map
                         #(str % \= (rec %))
                         (sort (normalize ks))))]
       (println "make-id" s)
       (digest/md5 s)))
  ([rec] (make-id rec (keys rec))))

;;TODO: memoize (?)
(defn find-answer [question ks]
  (println "find-answer" (make-id question ks))
  (mc/find-one-as-map "answers" {:_id (make-id question ks)}))

;;TODO: need to support ways to categorize results
(defn find-worker-results [worker-id]
  (mc/find-maps "results" {:WorkerId worker-id}))

;; Example answer:
(comment
{:_id "2e40781b3fd1fd65ff54cac0272bdc5d",
 :expected "correct",
 :question
 {:new-tel "+302103254747",
  :country "Greece",
  :locality "Αθήνα",
  :addr "Χαβρίου 8",
  :name "Τακοσ Γρηγοριοσ",
  :factual-id "1b903567-c695-43fb-b7d8-0e7493e163f0"},
 :notable-keys
 ["locality" "new-tel" "name" "country" "addr" "factual-id"]}
)

(defn save-answer
  "Saves the specified answer record.

   The record's ID will be generated as a question hash derived from the
   entries in :question
   attr names. This means it may overwrite an existing record, which would
   indicate we already had saved an answer for the same question.

   Answer structure:
     {:_id          [id derived from hashing notable entries]
      :expected     [expected answer value]
      :question     [input data used to form the corresponding question]
      :notable-keys [the critical question keys]}

   An answer could also be thought of as 'gold standard data'."
  [answer]
  (mc/save "answers"
           (assoc answer :_id  (make-id (:question answer) (:notable-keys answer)))))

(defn load-answers
  "Loads a batch of answer records from the specified file.
   Expects one record per line, formatted as JSON.
   There must be an _EXPECTED_ attribute, holding the correct answer for
   every record.

   notable-keys indicates the keys in the answer that define the essence of
   the corresponding question."
  [file notable-keys]
  (with-open [rdr (clojure.java.io/reader file)]
    (doseq [line (line-seq rdr)]
      (let [rec (json/parse-string line)]
        (save-answer
         {:expected (rec "_EXPECTED_")
          :question (dissoc rec "_EXPECTED_")
          :notable-keys notable-keys})))))

(defn save-result
  "Saves the specified result record.
   The HITId attribute is used for record's ID."
  [rec]
  (mc/save "results" (assoc rec :_id  (rec "HITId"))))

(defn split-by-key-pre [m pre]
  (let [groups (group-by #(.startsWith (key %) pre) m)]
    {:with-pre    (into {} (groups true))
     :without-pre (into {} (groups false))}))

(defn no-key-pre [m pre]
  (into {}
        (for [[k v] m]
          (if (.startsWith k pre)
            [(.substring k (count pre)) v]
            [k v]))))

(defn result-rec
  "A raw results record will have a ton of MTurk specific data, e.g. HITTypeId, WorkerId, etc.
   It will also have an arbitrary set of Answer attributes, headered by Answer.X, where X is
   the answer attribute name from the task loaded into MTurk. Same for Input.

   Mongo won't allow dots in key names. Also, we'd like to have the answer set and input set
   broken out. So we break out the Answer set and Input set into separate hash-maps, also
   stripping out the offending key prefixes.

   We rename the concept of Input to be :question.

   Returned hash-map is the core result record, with a broken-out :question and :answers, like:
   {'HITTypeID' blah
    'WorkerId' blahblah ...}
    :question {'attrA' 'valA' 'attrB' 'valB' ...}
    :answer   {'attr1' 'val1' 'attr2' 'val2' ...}"
  [raw]
  (let [{rec :without-pre answer :with-pre} (split-by-key-pre raw "Answer.")
        {rec :without-pre input :with-pre} (split-by-key-pre rec "Input.")]
    (assoc rec
      :question (no-key-pre input "Input.")
      :answer   (no-key-pre answer "Answer."))))

(defn load-results
  "Loads a batch of results from the specified file.
   Expects the file to be a CSV file, following MTurk's batch results download conventions.

   The HITId attribute is used for record IDs."
  [file]
  (with-open [rdr (clojure.java.io/reader file)]
    (let [recs (csv/parse-csv rdr)
          header (first recs)
          recs (rest recs)]
      (doseq [rec recs]
        (save-result (result-rec (zipmap header rec)))))))

(def NOTABLE-KEYS
  [:locality :new-tel :name :country :addr :factual-id])

(defn reset!
  ([k]
     (mc/drop "answers")
     (mc/drop "results")
     (condp = k
       :one (do
              (load-answers "answer.json" [:locality :new-tel :name :country :addr :factual-id])
              (load-results "result.csv"))
       :all (do
               (load-answers "gold-standard-tel-moderation.json" NOTABLE-KEYS)
               (load-results "tel_mod_results.csv"))))
  ([]
     (reset! :all)))

(defn tally
  "acc looks like:
     {:right [amt-right]
      :wrong [amt-wrong]}"
  [acc {:keys [expected actual]}]
  (println "EXP:" expected "ACT:" actual)
  (update-in acc
             [(if (= actual expected) :right :wrong)]
             inc))

;; Example answer:
(comment
{:_id "2e40781b3fd1fd65ff54cac0272bdc5d",
 :expected "correct",
 :question
 {:new-tel "+302103254747",
  :country "Greece",
  :locality "Αθήνα",
  :addr "Χαβρίου 8",
  :name "Τακοσ Γρηγοριοσ",
  :factual-id "1b903567-c695-43fb-b7d8-0e7493e163f0"},
 :notable-keys
 ["locality" "new-tel" "name" "country" "addr" "factual-id"]}
)
(defn accuracy
  "Returns an accuracy score, given a sequence of pairs, where each pair is a hash-map
   containing an :expected value and an :actual value."
  [pairs]
  (reduce tally {:right 0 :wrong 0} pairs))

(defn evaluation-pairs
  "Returns a sequence of pairs for accuracy evaluation based on results and
   notable-keys. For each result, we look for an existing answer in our db,
   based on notable-keys hashing.

   For each result/answer pair, the returned sequence will contain a hash-map
   with :expected and :actual.

   If an existing answer is not found for a result, we skip the result, meaning
   it's not to be tallied."
  [results notable-keys]
  (filter identity
          (map
           (fn [result]
             (when-let [answer (find-answer (:question result) notable-keys)]
               (println "found answer:" answer)
               {:expected (:expected answer)
                ;;todo: this will change depending on work design
                :actual   (get-in result [:answer :answer])}))
           results)))

(defn evaluate-worker [worker-id notable-keys]
  (accuracy (evaluation-pairs (find-worker-results worker-id) notable-keys)))

;;TODO: we should have a worker collection, but how to keep up to date?
(defn all-worker-ids []
  (into #{}
        (map :WorkerId
             (mc/find-maps "results" {} ["WorkerId"]))))

(defn evaluate-workers []
  (reduce
   (fn [acc wid]
     (assoc acc wid (evaluate-worker wid NOTABLE-KEYS)))
   {}
   (all-worker-ids)))

(comment
[:locality :new-tel :name :country :addr :factual-id]

)

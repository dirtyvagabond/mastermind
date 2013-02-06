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

;;TODO: make-id approach should somehow take spec into consideration (?)
(defn save-answer
  "Saves the specified answer record. The record's ID will be generated as a
   question hash derived from all attr names. This means it may overwrite an existing
   matching record."
  [rec notable-keys]
  (println "save-answer ID:" (make-id rec notable-keys))
  (mc/save "answers" {:_id  (make-id rec notable-keys)
                      :data rec
                      :notable-keys notable-keys}))

(defn load-answers
  "Loads a batch of answer records from the specified file.
   Expects one record per line, formatted as JSON.
   There must be an _EXPECTED_ attribute, holding the correct answer for
   every record.

   notable-keys indicates the keys in the answer that define the essence of
   the corresponding question"
  [file notable-keys]
  (with-open [rdr (clojure.java.io/reader file)]
    (doseq [line (line-seq rdr)]
      (save-answer (json/parse-string line) notable-keys))))

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

(defn reset!!! []
  (mc/drop "answers")
  (mc/drop "results")

  ;;(load-answers "answer.json" [:locality :new-tel :name :country :addr :factual-id])
  ;;(load-results "result.csv")

  (load-answers "gold-standard-tel-moderation.json" NOTABLE-KEYS)
  (load-results "tel_mod_results.csv")

  )


(defn accuracy-score
  "Returns an accuracy score, Given submitted results and the expected correct answers."
  [results answers]


  )

(defn evaluate-worker [worker-id]
  (let [results (find-worker-results worker-id)]
    (doseq [res results]
      (let [answer (find-answer (:question res) NOTABLE-KEYS)]

        (println "FID:" (get-in answer [:data :factual-id]))
        )

      )

    )


  )


(comment
[:locality :new-tel :name :country :addr :factual-id]

)

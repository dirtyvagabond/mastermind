(ns mastermind.db
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [digest :as digest]
            [clojure-csv.core :as csv]
            [cheshire.core :as json]))

(def ^:private work-specs-atom (atom {}))

;; Document collections
(def VERITIES-COLL "verities")
(def RESULTS-COLL "results")


(defn get-all-work-specs
  "Returns a hash-map of work-spec-name -> work-spec, loaded from db.
   Top level keys are expected to be keywords."
  []
  (reduce
   (fn [acc spec]
     (assoc acc (keyword (:name spec)) spec))
   {}
   (mc/find-maps "work-specs" {})))

(defn load-work-specs!
  "Loads in-memory cache of work specs."
  []
  (reset! work-specs-atom (get-all-work-specs)))

(defn init! []
  (mg/connect!)
  (mg/set-db! (mg/get-db "monger-test"))
  (load-work-specs!))

(defn save-work-spec!
  "Saves work-spec to the db and updates internal cache of work specs.

   A work spec describes the kind of work being done. It must have:
     unique :name
     :notable-keys vector

   Example:
     {:name         :tel-edit
      :notable-keys [:locality :new-tel :name :country :addr :factual-id]
      :description  'Determine whether a proposed business telephone edit is correct.'}"
  [work-spec]
  (mc/save "work-specs" (assoc work-spec :_id (:name work-spec)))
  (swap! work-specs-atom assoc (:name work-spec) work-spec))

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
       (digest/md5 s)))
  ([rec] (make-id rec (keys rec))))

(defn get-notable-keys
  "Gets the notable keys associated with work-spec-name, from cached work specs.
   Throws IllegalArgumentException if there are no cached work-specs with work-spec-name."
  [work-spec-name]
  (let [wsname (keyword work-spec-name)]
    (if-let [work-spec (@work-specs-atom wsname)]
      (:notable-keys work-spec)
      (throw (IllegalArgumentException. (str "Could not find work spec named " wsname ". all specs:" @work-specs-atom))))))

;;TODO: memoize (?)
(defn find-verity [question ks]
  (mc/find-one-as-map VERITIES-COLL {:_id (make-id question ks)}))

(defn find-worker-results [worker-id]
  (mc/find-maps RESULTS-COLL {:WorkerId worker-id}))

(defn save-verity
  "Saves the specified verity record.

   The verity's ID will be generated based on the notable entries of the verity's underlying
   question. This means it may overwrite an existing verity record, which would
   indicate we already had saved a verity for the same question.

   verity record structure:
     {:_id          [id derived from hashing notable entries]
      :expected     [the answer that is conidered correct for the verity's underlying question]
      :question     [input data representing the underlying question]
      :notable-keys [the critical question keys]
      :provenance   [a structure that details verity origin]}"
  [verity]
  (mc/save VERITIES-COLL
           (assoc verity :_id (make-id (:question verity) (:notable-keys verity)))))

(defn load-verities
  "Loads a batch of verity records from the specified file into the db.
   Expects one record per line, formatted as JSON.
   There must be an _EXPECTED_ attribute, holding the correct answer for
   every record."
  [file work-spec-name]
  (let [work-specs (get-all-work-specs)]
    (with-open [rdr (clojure.java.io/reader file)]
      (doseq [line (line-seq rdr)]
        (let [rec (json/parse-string line)
              provenance {:file (str file)
                          :host (.getHostName (java.net.InetAddress/getLocalHost))
                          :time (.getTime (java.util.Date.))}]
          (save-verity
           {:expected (rec "_EXPECTED_")
            :question (dissoc rec "_EXPECTED_")
            :notable-keys (get-notable-keys work-spec-name)
            :provenance provenance}))))))

(defn save-result
  "Saves the specified result record.
   The AssignmentID attribute is used for record's ID.
   rec should include an entry for :work-spec-name"
  [rec]
  (mc/save "results" (assoc rec :_id  (rec "AssignmentId"))))

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
  "Returns a proper result record formed from the raw data coming from an
   incoming MTurk assignment.

   The raw assignment data from MTurk will have a ton of MTurk specific data,
   e.g. HITTypeId, WorkerId, etc. It will also have an arbitrary set of Answer
   attributes, headered by Answer.X, where X is the answer attribute name from
   the task loaded into MTurk. Same for Input.

   Mongo won't allow dots in key names. Also, we'd like to have the answer set and input set
   broken out. So we break out the Answer set and Input set into separate hash-maps, also
   stripping out the offending key prefixes.

   We rename the concept of Input to be :question.

   Returned hash-map is the core result record, with a broken-out :question and :answer, like:
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
  "Loads a batch of results from the specified file into the db.
   Expects the file to be a CSV file, following MTurk's batch results download conventions.

   The HITId attribute is used for record IDs."
  [file work-spec-name]
  (with-open [rdr (clojure.java.io/reader file)]
    (let [recs (csv/parse-csv rdr)
          header (first recs)
          recs (rest recs)]
      (doseq [rec recs]
        (save-result
         (assoc (result-rec (zipmap header rec))
                :work-spec-name work-spec-name))))))

(defn tally
  "acc looks like:
     {:right [amt-right]
      :wrong [amt-wrong]}"
  [acc {:keys [expected actual]}]
  (update-in acc
             [(if (= actual expected) :right :wrong)]
             inc))

(defn summarize-results
  "Runs a summarization of all results that were submitted for the
   HIT specified by hit-id.

   answer-fn is the function to apply to the :answer of the result
   in order to pull out the actual value submitted by the turker."
  [hit-id answer-fn]
  (let [results (mc/find-maps RESULTS-COLL {:HITId hit-id})]
    (frequencies (map (comp :answer answer-fn) results))))

(defn accuracy
  "Returns an accuracy score, given a sequence of pairs, where each pair is a hash-map
   containing an :expected value and an :actual value."
  [pairs]
  (reduce tally {:right 0 :wrong 0} pairs))

(defn evaluation-pairs
  "Returns a sequence of pairs for accuracy evaluation based on results and
   notable-keys. For each result, we look for an existing verity in our db,
   based on notable-keys hashing.

   For each result/verity pair, the returned sequence will contain a hash-map
   with :expected and :actual.

   If an existing verity is not found for a result, we skip the result, meaning
   it's not tallied."
  [results]
  (filter identity
          (map
           (fn [result]
             (when-let [verity (find-verity (:question result)
                                            (get-notable-keys (:work-spec-name result)))]
               {:expected (:expected verity)
                ;;TODO: this will change depending on work design
                :actual   (get-in result [:answer :answer])}))
           results)))

(defn evaluate-worker [worker-id]
  (accuracy (evaluation-pairs (find-worker-results worker-id))))

;;TODO: we should have a worker collection, but how to keep up to date?
(defn all-worker-ids []
  (into #{}
        (map :WorkerId
             (mc/find-maps RESULTS-COLL {} ["WorkerId"]))))

(defn evaluate-workers []
  (reduce
   (fn [acc wid]
     (assoc acc wid (evaluate-worker wid)))
   {}
   (all-worker-ids)))

(defn reload!
  ([k]
     (init!)
     (save-work-spec! {:name         :tel-edit
                      :notable-keys [:locality :new-tel :name :country :addr :factual-id]
                      :description  "Determine whether a proposed business telephone edit is correct."})
     (mc/drop VERITIES-COLL)
     (mc/drop RESULTS-COLL)
     (condp = k
       :one (do
              (load-verities "answer.json" :tel-edit)
              (load-results "result.csv" :tel-edit))
       :all (do
              (load-verities "gold-standard-tel-moderation.json" :tel-edit)
              (load-results "tel_mod_results.csv" :tel-edit))))
  ([]
     (reload! :all)))


(comment
  ;;
  ;; Example answer structure, as it would appear in db:
  ;;
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

  ;;
  ;; Example result structure, as it would appear in db:
  ;;
  {:SubmitTime "Mon Jan 07 02:11:22 GMT 2013",
   :RequesterAnnotation "BatchId:1001725;",
   :AutoApprovalDelayInSeconds "3600",
   :AcceptTime "Mon Jan 07 02:09:46 GMT 2013",
   :Description
   "Given specific business information, determine whether the phone number is correct for that business",
   :AssignmentDurationInSeconds "420",
   :NumberOfSimilarHITs "",
   :MaxAssignments "3",
   :LifetimeInSeconds "",
   :question
   {:locality "Αθήνα",
    :new-tel "+302103254747",
    :name "Τακοσ Γρηγοριοσ",
    :country "Greece",
    :addr "Χαβρίου 8",
    :factual-id "1b903567-c695-43fb-b7d8-0e7493e163f0",
    :question-id
    "1b903567-c695-43fb-b7d8-0e7493e163f0/2012-12-10T08:43:03.529000Z"},
   :answer
   {:why_not_sure "unable to locate online or facebook",
    :source "",
    :answer "not sure"},
   :WorkerId "A3NUWV8UAHEDQ1",
   :AssignmentStatus "Approved",
   :RejectionTime "",
   :CreationTime "Fri Jan 04 21:01:24 GMT 2013",
   :RequesterFeedback "",
   :Last30DaysApprovalRate "100% (66/66)",
   :HITId "2XGQ4J3NAB6B6V44BFPIZ1TKC40ER0",
   :AssignmentId "2M8J2D21O3W5OQP8C6JI8KYIRZPE8I",
   :WorkTimeInSeconds "96",
   :ApprovalTime "2013/01/07 03:12:03 +0000",
   :_id "2XGQ4J3NAB6B6V44BFPIZ1TKC40ER0",
   :Title "Verify if a phone number is correct",
   :AutoApprovalTime "Mon Jan 07 03:11:22 GMT 2013",
   :LifetimeApprovalRate "100% (66/66)",
   :work-spec-name "tel-edit",
   :Keywords "data collection, telephone, verify",
   :Reward "$0.10",
   :HITTypeId "2FYCOW0LGRZ3ZLG48TXVTUZWD358ZQ",
   :Expiration "Tue Jan 08 09:01:24 GMT 2013",
   :Last7DaysApprovalRate "100% (66/66)"}
)
(ns mastermind.moderate-phones
  (:import [java.io BufferedReader FileReader]
           [java.net URLEncoder])
  (:require [clojure.string :as str]
            [clojure-csv.core :as csv]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [factql.core :as facts]
            [digest :as digest]))


;;(def CC-FILE "moderate-phones.csv")

;;(def RESULTS-FILE "Batch_972607_batch_results.cc.csv")


;;
;; Tasks and HITs
;;

;; We will need to query Factual for head data when forming tasks
(facts/init!)

;; Master map of Factual's country codes => Human readable country name
(defn get-country-codes []
  (let [lines (str/split (slurp "country-codes.csv") #"\n")]
    (reduce
     (fn [accum line]
       (let [[name code] (str/split line #",")]
         (if (not (empty? code))
           (assoc accum (str/lower-case code) name)
           accum)))
     {}
     lines)))

(defn get-head-data [factual-id]
  (select-keys
   (first (facts/select global (where (= :factual_id factual-id))))
   [:country :name :locality :address]))

(defn +country-names [mods]
  (let [codes (get-country-codes)]
    (letfn [(+cntryname [m]
              (assoc m :country-name (codes (:country m))))]
      (map +cntryname mods))))

;; TODO: MTurke task expects "country" to be from rec's :country-code
#_(defn q-from-mod [rec]
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

(defn whole? [task]
  (and
    (get task :name)
    (get task :address)
    (get task :locality)
    (get task :country-name)))

;; TODO: MTurk task expects "country" to be from rec's :country-code
;; TODO: figure out all the ids we need, and handle properly
;;         Stitch's edit it, some MTurk run id, the canon'd question-id, HITid
#_(defn task-from-raw [mod]
  "mod must be the raw mod data from Stitch"
  (let [factual-id (get mod "factual_id")
        head (get-head-data factual-id)
        name (get-in mod ["meta" "rawPayload" "vendor" "originalPlace" "business" "name"])
        addr (get-in mod ["meta" "rawPayload" "vendor" "originalPlace" "address" "formattedAddressLines"])
        corr (get-in mod ["meta" "rawPayload" "reviewer" "dataProblem" "correctedField"])
        new-tel (get (first corr) "correctedValue")]
    (merge
     head
     {:id (get mod "_id")
      :factual-id factual-id
      :name name
      :address (str/join ", " addr)
      :new-tel new-tel})))

(defn raw-task-from-raw-mod [mod]
  (let [factual-id (get mod "factual_id")
        name (get-in mod ["meta" "rawPayload" "vendor" "originalPlace" "business" "name"])
        addr (get-in mod ["meta" "rawPayload" "vendor" "originalPlace" "address" "formattedAddressLines"])
        corr (get-in mod ["meta" "rawPayload" "reviewer" "dataProblem" "correctedField"])
        new-tel (get (first corr) "correctedValue")]
    {:edit-id (get mod "_id")
     :factual-id factual-id
     :name name
     :address (str/join ", " addr)
     :new-tel new-tel}))

#_(defn get-tasks
  "Returns the set of unique tasks read from json-file. Each task will be a hash-map
   with all raw task attributes, plus the attributes used by our MTurk template:
     :id
     :tel

   Takes only 100"
  [json-file]
  (let [recs (json/parse-string (slurp json-file))
        recs (take 100 recs)]
    (filter whole?
            (map q-from-mod recs))))

(defn get-raw-mods [json-file]
  (json/parse-string (slurp json-file)))

(defn get-tasks [json-file]
  (let [tasks (map raw-task-from-raw-mod (get-raw-mods json-file))]
    tasks
    ))

(defn hits-csv
  "Returns the full CSV content for an MTurk hits file."
  [tasks]
  (str "task-id,factual-id,name,addr,locality,country,new-tel\n"
       (csv/write-csv (map
                       (fn [{:keys [task-id factual-id name address locality country new-tel]}]
                         [task-id factual-id name address locality country new-tel])
                       tasks))))

(defn create-hits-file [hits-file mod-file]
  (spit hits-file (hits-csv (get-tasks mod-file))))

(defn gold-from-tsv
  "Loads in telephone moderation gold data from a TSV input file, assuming this
   column ordering:
     factual-id name address locality country new-tel answer

   Assumes the first line in the file is a header line and skips it.

   country values will be like 'Greece', 'Croatia', etc.

   Potential values of correct.answer: 'correct' | 'not correct'"
  [file]
  (let [cols [:factual-id :name :address :locality :country :new-tel :answer]
        lines (rest (str/split (slurp file) #"\n"))]
    (map
     (fn [line]
       (let [parts (str/split line #"\t")]
         (into {} (map-indexed (fn [idx itm]
                                 (let [k (get cols idx)]
                                   [k (get parts idx)  ]))
                               parts))))
     lines)))

;;TODO: need to do this intelligently, for many tasks across many projects, etc.
;;TODO: normalize keys
(defn make-id
  "Creates a consistent task id based on the specified key/val pairs of hash-map rec."
  [rec ks]
  (let [s (apply str (map
                      #(str (name %) \= (rec %))
                      (sort ks)))]
  (digest/md5 s)))

(defn write-hits-file-from-gold [gold-recs hits-file]
  (spit hits-file (hits-csv gold-recs)))

(defn gold-tels [file]
  (let [gold (gold-from-tsv file)]
    ;;make sure there's a task-id, derived from task's main data
    (map #(assoc % :task-id (make-id % [:name :address :locality :country :new-tel :answer])) gold)))

(defn create-hits-file-from-gold-file [hits-file gold-file]
  (let [gold (gold-tels "gold-standard-tel-moderation.tsv")]
    (spit hits-file (hits-csv gold))))

(defn seeded-tasks
  "gold:  gold data to mix in with tasks
   real:  the real tasks to be done
   ratio: how many real records to go with each gold record"
  [gold real real-per-gold]
  (flatten (interleave (partition real-per-gold real) gold)))

;; (get-tasks mod-file)
(defn go []
  (create-hits-file-from-gold-file "tel-hits.csv" "gold-standard-tel-moderation.tsv"))

(defn go []
  (let [gold (gold-tels "gold-standard-tel-moderation.tsv")
       ]
;;(spit hits-file (hits-csv gold))

    )

  )
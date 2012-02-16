;; Copyright (c) Jonas Enlund. All rights reserved.  The use and
;; distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

;; Modified to run under ClojureCLR by Rob Rowe
;; Copyright (c) Rob Rowe, 2012. All rights reserved.  The use and
;; distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(ns ^{:author "Jonas Enlund, modified for ClojureCLR by Rob Rowe"
      :doc "Reading and writing comma separated values."}
  cljclr.data.csv                                                          ;RR: clojure.data.csv
  (:require (clojure [string :as str]))
					
  (:import (System.IO TextReader TextWriter StringReader EndOfStreamException))) ;RR: (:import (java.io Reader Writer StringReader EOFException)))

;(set! *warn-on-reflection* true)

;; Reading

(def ^{:private true} lf  (int \newline))
(def ^{:private true} cr  (int \return))
(def ^{:private true} eof -1)

(defn- read-quoted-cell [^TextReader reader ^StringBuilder sb sep quote]     ;RR: ^Reader
  (loop [ch (.Read reader)]                                                    ;RR: .read
    (condp == ch
      quote (let [next-ch (.Read reader)]                                      ;RR: .read
              (condp == next-ch
                quote (do (.Append sb (char quote))                            ;RR: .append
                          (recur (.Read reader)))                              ;RR: .read
                sep :sep
                lf  :eol
                cr  (if (== (.Read reader) lf)                                 ;RR: .read
                      :eol
                      (throw (Exception. ^String (format "CSV error (unexpected character: %c)" next-ch))))
                eof :eof
                (throw (Exception. ^String (format "CSV error (unexpected character: %c)" next-ch)))))
      eof (throw (EndOfStreamException. "CSV error (unexpected end of file)")) ;RR: EOFException
      (do (.Append sb (char ch))                                               ;RR: .append
          (recur (.Read reader))))))                                           ;RR: .read

(defn- read-cell [^TextReader reader ^StringBuilder sb sep quote]              ;RR: [^Reader
  (let [first-ch (.Read reader)]                                               ;RR: .read
    (if (== first-ch quote)
      (read-quoted-cell reader sb sep quote)
      (loop [ch first-ch]
        (condp == ch
          sep :sep
          lf  :eol
          cr (let [next-ch (.Read reader)]                                     ;RR: .read
               (if (== next-ch lf)
                 :eol
                 (do (.Append sb \return)                                      ;RR: .append
                     (recur next-ch))))
          eof :eof
          (do (.Append sb (char ch))                                           ;RR: .append
              (recur (.Read reader))))))))                                     ;RR: .read

(defn- read-record [reader sep quote]
  (loop [record (transient [])]
    (let [cell (StringBuilder.)
          sentinel (read-cell reader cell sep quote)]
      (if (= sentinel :sep)
        (recur (conj! record (str cell)))
        [(persistent! (conj! record (str cell))) sentinel]))))

(defprotocol Read-CSV-From
  (read-csv-from [input sep quote]))

(extend-protocol Read-CSV-From
  String
  (read-csv-from [s sep quote]
    (read-csv-from (StringReader. s) sep quote))    ;RR: Added System.IO 

  StringReader
  (read-csv-from [reader sep quote]
		 (lazy-seq
		   (let [[record sentinel] (read-record reader sep quote)]
       (case sentinel
	 :eol (cons record (read-csv-from reader sep quote))
	 :eof (when-not (= record [""])
		(cons record nil))))))

		   
  TextReader                                                ;RR: Reader
  (read-csv-from [reader sep quote] 
    (lazy-seq
     (let [[record sentinel] (read-record reader sep quote)]
       (case sentinel
	 :eol (cons record (read-csv-from reader sep quote))
	 :eof (when-not (= record [""])
		(cons record nil)))))))

(defn read-csv
  "Reads CSV-data from input (String or java.io.Reader) into a lazy
  sequence of vectors.

   Valid options are
     :separator (default \\,)
     :quote (default \\\")"
  [input & options]
  (let [{:keys [separator quote] :or {separator \, quote \"}} options]
    (read-csv-from input (int separator) (int quote))))


;; Writing

(defn- write-cell [^TextWriter writer obj sep quote quote?]          ;RR: ^Writer
  (let [string (str obj)
	must-quote (quote? string)]
    (when must-quote (.Write writer (int quote)))                    ;RR: .write
    (.Write writer (if must-quote                                    ;RR: .write
		     (str/escape string
				 {quote (str quote quote)})
		     string))
    (when must-quote (.write writer (int quote)))))

(defn- write-record [^TextWriter writer record sep quote quote?]     ;RR: ^Writer
  (loop [record record]
    (when-first [cell record]
      (write-cell writer cell sep quote quote?)
      (when-let [more (next record)]
	(.Write writer sep)                                    ;RR: .write writer (int sep)
	(recur more)))))

(defn- write-csv*
  [^TextWriter writer records sep quote quote? ^String newline]      ;RR: ^Writer
  (loop [records records]
    (when-first [record records]
      (write-record writer record sep quote quote?)
      (.Write writer newline)                                        ;RR: .write
      (recur (next records)))))

(defn write-csv
  "Writes data to writer in CSV-format.

   Valid options are
     :separator (Default \\,)
     :quote (Default \\\")
     :guote? (A predicate function which determines if a string should be quoted. Defaults to quoting only when necessary.)
     :newline (:lf (default) or :cr+lf)"
  [writer data & options]
  (let [opts (into {} options)
        separator (or (:separator opts) \,) 
        quote (or (:quote opts) \")
        quote? (or (:quote? opts) #(some #{separator quote \return \newline} %))
        newline (or (:newline opts) :lf)]
    (write-csv* writer
		data
		separator
		quote
                quote?
		({:lf "\n" :cr+lf "\r\n"} newline))))

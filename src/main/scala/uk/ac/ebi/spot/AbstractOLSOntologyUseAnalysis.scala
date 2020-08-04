package uk.ac.ebi.spot

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{date_format, month, regexp_extract, to_date, year}

abstract class AbstractOLSOntologyUseAnalysis (private val sparkSession: SparkSession, private val logFilesToRead: String)
  extends WebLogAnalysis [OntologyUseLogFormat] {

  val logger = Logger[AbstractOLSOntologyUseAnalysis]
  val ontologiesToAnalise:List[String] = List[String]("bfo", "obi", "doid", "chebi", "zfa", "xao", "amphx", "aeo",
    "apollo_sv", "go", "apo", "aro", "bco", "agro", "bspo", "pato", "caro", "pr", "cido", "bto",

   "cheminf", "cdao", "chiro", "cob", "po", "cro", "cteno", "ddanat", "cvdo", "ddpheno", "cl", "dideo",
    "dron", "ecocore", "chmo", "duo", "dpo", "cmo", "emapa", "eco",

    "eupath","exo", "ecto", "fao", "fbbi", "ero", "fbcv", "flopo", "fma", "fbdv", "fypo", "foodon",
    "gaz", "fovt", "genepio", "geno", "fbbt", "geo", "hancestro", "htn",

    "gno", "envo", "hao", "iceo", "iao", "ico", "ido", "ino", "labo", "hom", "ma", "hsapdv", "mfoem",
    "mfomd", "maxo", "miapa", "mi", "mco", "clyh", "mf",

    "mfmo", "clo", "micro", "mod", "mmo", "mop", "mpio", "mro", "nbo", "mondo", "ms", "ncbitaxon",
    "ncro", "oae", "ncit", "mpath", "nomen", "ogg", "oarcs", "obcs",

  "obib", "ogsf", "oba", "ogms", "ohd", "ohmi", "mp", "olatdv", "omiabis", "omit", "ohpi", "omp", "omo",
    "mmusdv", "oostt", "omrse", "opmi", "opl", "ornaseq", "ovae",

    "ontoneo", "pdro", "peco", "phipo", "plana", "planp", "pco", "ppo", "pdumdv", "rs", "psdo", "pw",
    "poro", "so", "spd", "rxno", "swo", "stato", "taxrank", "sepio",

    "tto", "symp", "to", "ro", "trans", "vo", "uo", "vt", "vto", "upheno", "wbbt", "xco", "wbls", "zeco",
    "wbphenotype", "cio", "kisao", "hp", "xpo", "mamo",

    "uberon", "sbo", "txpo", "fobi", "zfs", "xlmod","zp"
  )

  val isAnOntologyToInclude = (ontologyToCheck: String) => ontologiesToAnalise.contains(ontologyToCheck)

  def readLogFiles(): Dataset[Row] = {
    val baseDataFrame = sparkSession.read.text(logFilesToRead)
    logger.trace("Number of logfile lines = " + baseDataFrame.count())
    val withoutBotsDataFrame = baseDataFrame.filter(l =>
      !l.getString(0).contains("bot") && !l.getString(0).contains("crawler") &&
        !l.getString(0).contains("root") && !l.getString(0).contains("graph"))
    logger.trace("Number of logfile lines without bots = " + withoutBotsDataFrame.count())

    withoutBotsDataFrame.show(10, false)
    withoutBotsDataFrame
  }

  /**
   * /ontologies/{onto}/terms
   * /ontologies/{onto}/properties
   * /ontologies/{onto}/individuals
   *
   * @param dataset
   * @return
   */
  def parseDataset(dataset: Dataset[Row]): Dataset[OntologyUseLogFormat] = {
    import sparkSession.implicits._

    sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    // See https://stackoverflow.com/questions/7015715/a-regex-pattern-for-different-tomcats-log-entries/7073775
    val regEx: String = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3} )?(\\S+) (\\S+) \\[(\\d\\d/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2})\\s\\S\\d{4}\\] \"(.*?)\" (\\S+) (\\S+)( \"(.*?)\" \"(.*?)\")?"
    val requestRegEx: String = "\\w+/ontologies/(\\w+)/(\\w*)/?"

    val parsedDataFrame = dataset.select(
      regexp_extract($"value", regEx, 1).as("remoteIPAddress"),
      regexp_extract($"value", regEx, 4).as("dateTime"),
      month(to_date(regexp_extract($"value", regEx, 4), "dd/MMM/yyyy")).as("month"),
      year(to_date(regexp_extract($"value", regEx, 4), "dd/MMM/yyyy")).as("year"),
      regexp_extract($"value", regEx, 5).as("request"),

      regexp_extract(regexp_extract($"value", regEx, 5), requestRegEx, 1).as("ontology"),
      regexp_extract(regexp_extract($"value", regEx, 5), requestRegEx, 2).as("requestType"),

      regexp_extract($"value", regEx, 6).as("status"),
      regexp_extract($"value", regEx, 7).as("bytes"),
      regexp_extract($"value", regEx, 9).as("referer"),
      regexp_extract($"value", regEx, 10).as("userAgent")
    )
    println("#################  parseDataset")
    parsedDataFrame.show(10, false)
    parsedDataFrame.as[OntologyUseLogFormat]
  }
}


case class OntologyUseLogFormat(remoteIPAddress: String, dateTime: String, month: Integer, year: Integer, request: String,
                                status: String, bytes: String, referer: String, userAgent: String,
                                ontology: String, requestType: String)
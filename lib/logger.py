class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "bhavesh.spark.reatil_etl"
        conf = spark.sparkContext.getConf()
        appName = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "." + appName)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warn(self, msg):
        self.logger.warn(msg)

    def debug(self, msg):
        self.logger.debug(msg)
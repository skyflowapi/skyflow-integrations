// Copyright (c) 2025 Skyflow, Inc.

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import kafka.KafkaToVault;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.PropertyUtil;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static final String TEMPLATE_PROPERTY_LONG_OPT = "templateProperty";

  private static final Option PROPERTY_OPTION =
      OptionBuilder.withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt(TEMPLATE_PROPERTY_LONG_OPT)
          .withDescription("Value for given property")
          .create();
  private static final Options options = new Options().addOption(PROPERTY_OPTION);

  /**
   * Parse command line arguments
   *
   * @param args command line arguments
   * @return parsed arguments
   */
  public static CommandLine parseArguments(String... args) {
    CommandLineParser parser = new DefaultParser();
    LOGGER.info("Parsing arguments {}", (Object) args);
    try {
      return parser.parse(options, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  public static void main(String... args)
      throws StreamingQueryException, TimeoutException, SQLException, InterruptedException {
    KafkaToVault kafkaJob = createAndRegisterProperties(args);
    runSparkJob(kafkaJob);
  }

  /**
   * Creates KafkaToVault instance and registers any properties passed on the command line.
   *
   * @param args Command line args
   * @return the constructed KafkaToVault object
   */
  static KafkaToVault createAndRegisterProperties(String... args) {
    String[] remainingArgs;
    try {
      CommandLine cmd = parseArguments(args);
      Properties properties = cmd.getOptionProperties(TEMPLATE_PROPERTY_LONG_OPT);
      remainingArgs = cmd.getArgs();

      LOGGER.info("Properties: {}", properties);
      LOGGER.info("Remaining args: {}", (Object) remainingArgs);
      PropertyUtil.registerProperties(properties);
    } catch (IllegalArgumentException e) {
      LOGGER.error(e.getMessage(), e);
      throw e;
    }

    return new KafkaToVault();
  }

  /** Run spark job for KafkaToVault. */
  static void runSparkJob(KafkaToVault kafkaJob)
      throws IllegalArgumentException,
          StreamingQueryException,
          TimeoutException,
          SQLException,
          InterruptedException {
    LOGGER.debug("Validating input parameters");
    kafkaJob.validateInput();
    LOGGER.debug("Start runSparkJob");
    kafkaJob.runTemplate();
    LOGGER.debug("End runSparkJob");
  }
}

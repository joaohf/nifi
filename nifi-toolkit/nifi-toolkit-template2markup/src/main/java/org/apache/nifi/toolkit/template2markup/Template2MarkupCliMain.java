/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.toolkit.template2markup;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintStream;

public class Template2MarkupCliMain {
    public static final String INPUT_TEMPLATE_FILE_OPTION = "inputTemplateFile";
    public static final String OUTPUT_MARKUP_DIRECTORY_OPTION = "outputMarkupDirectory";
    public static final String HELP_OPTION = "help";

    /**
     * Prints the usage to System.out
     *
     * @param errorMessage optional error message
     * @param options      the options object to print usage for
     */
    public static void printUsage(String errorMessage, Options options) {
        if (errorMessage != null) {
            System.out.println(errorMessage);
            System.out.println();
            System.out.println();
        }
        System.out.println("template2markup is a command line tool that can read a template xml filee and convert to markup (asciidoctor)");
        System.out.println();
        System.out.println("bin/template2markup.sh -i example_template.xml -o markup_asciidoctor");
        System.out.println();
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(160);
        helpFormatter.printHelp("template2markup", options);
        System.out.flush();
    }

    /**
     * Parses command line options into a CliParse object
     *
     * @param options an empty options object (so callers can print usage if the parse fails
     * @param args    the string array of arguments
     * @return a CliParse object containing the constructed SiteToSiteClient.Builder and a TransferDirection
     * @throws ParseException if there is an error parsing the command line
     */
    public static CliParse parseCli(Options options, String[] args) throws ParseException {
        options.addOption("i", INPUT_TEMPLATE_FILE_OPTION, true, "Input template file");
        options.addOption("o", OUTPUT_MARKUP_DIRECTORY_OPTION, true, "Output directory");
        options.addOption("h", HELP_OPTION, false, "Show help message and exit");
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;
        commandLine = parser.parse(options, args);
        if (commandLine.hasOption(HELP_OPTION)) {
            printUsage(null, options);
            System.exit(1);
        }
        Template2MarkupConverter.Builder builder = new Template2MarkupConverter.Builder();

        if (commandLine.hasOption(INPUT_TEMPLATE_FILE_OPTION)) {
            builder.inputTemplateFile(commandLine.getOptionValue(INPUT_TEMPLATE_FILE_OPTION));
        } else {
            throw new ParseException("Must specify template input file");
        }
        if (commandLine.hasOption(OUTPUT_MARKUP_DIRECTORY_OPTION)) {
            builder.outputDirectory(commandLine.getOptionValue(OUTPUT_MARKUP_DIRECTORY_OPTION));
        } else {
            builder.outputDirectory("./outputMarkup");
        }
        return new CliParse() {
            @Override
            public Template2MarkupConverter.Builder getBuilder() {
                return builder;
            }
        };
    }

    public static void main(String[] args) {
        // Make IO redirection useful
        PrintStream output = System.out;
        System.setOut(System.err);
        Options options = new Options();
        try {
            CliParse cliParse = parseCli(options, args);
            try (Template2MarkupConverter c = cliParse.getBuilder().build()) {
                new Template2MarkupConverter(c).convert();
            }
        } catch (Exception e) {
            printUsage(e.getMessage(), options);
            e.printStackTrace();
        }
    }

    /**
     * Combines a SiteToSiteClient.Builder and TransferDirection into a return value for parseCli
     */
    public interface CliParse {
        Template2MarkupConverter.Builder getBuilder();
    }
}

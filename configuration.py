import platform
import os
import json
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import const

class Kind:
    QueryTime:str = const.QUERY_TIME_KEY
    CauseCode:str = const.CAUSE_CODE_KEY
    CpsMetric:str = const.CPS_METRIC_KEY

class Config:
    def __init__(self):
        self.FileCreateInterval:int = const.FILE_CREATE_INTERVAL
        self.QueryTimeInterval:int = const.QUERY_TIME_AGGR_INTERVAL
        self.CauseCodeInterval:int = const.CAUSE_CODE_AGGR_INTERVAL
        self.CPSMetricInterval:int = const.CPS_METRIC_AGGR_INTERVAL
        self.CreateFileWaitDuration:int = const.FILE_CREATE_WAIT_DURATION
        self.InputFolder:str = os.getcwd()
        self.OutputFolder:str = os.getcwd()
        self.InputFilePattern:str = const.INPUT_FILE_PATTERN
        self.OutputFilePattern:str = const.OUTPUT_FILE_PATTERN
        self.NodeName:str = platform.node()
        # On Demand Request
        self.OutputTypes = []
        self.OnDemandProcess:bool = False
        self.Kind = Kind()
        self.LogFolder = const.LOG_FOLDER_PATH

    def load(self, filename):
        fd = None
        err = ""
        try:
            self.FileCreateInterval:int = const.FILE_CREATE_INTERVAL
            self.QueryTimeInterval:int = const.QUERY_TIME_AGGR_INTERVAL
            self.CauseCodeInterval:int = const.CAUSE_CODE_AGGR_INTERVAL
            self.CPSMetricInterval:int = const.CPS_METRIC_AGGR_INTERVAL
            self.CreateFileWaitDuration:int = const.FILE_CREATE_WAIT_DURATION
            fd = open(filename, "rt")
            data = json.load(fd)
            if data:
                if data.get(const.INPUT_FOLDER):
                    self.InputFolder = data.get(const.INPUT_FOLDER)
                if data.get(const.OUTPUT_FOLDER):
                    self.OutputFolder = data.get(const.OUTPUT_FOLDER)
                if data.get(const.OUTPUT_PATTERN):
                    self.OutputFilePattern = data.get(const.OUTPUT_PATTERN)
                if data.get(const.NODE_NAME):
                    self.NodeName = data.get(const.NODE_NAME)
                if data.get(const.NEW_FILE_INTERVAL):
                    self.FileCreateInterval = data.get(const.NEW_FILE_INTERVAL)
                if data.get(const.QUERY_TIME_INTERVAL):
                    self.QueryTimeInterval = data.get(const.QUERY_TIME_INTERVAL)
                if data.get(const.CAUSE_CODE_INTERVAL):
                    self.CauseCodeInterval = data.get(const.CAUSE_CODE_INTERVAL)
                if data.get(const.CPS_METRIC_INTERVAL):
                    self.CPSMetricInterval = data.get(const.CPS_METRIC_INTERVAL)
                if data.get(const.LOG_FOLDER):
                    self.LogFolder = data.get(const.LOG_FOLDER)

                if data.get(const.TYPE):
                    kind = data.get(const.TYPE)
                    if const.QUERY_TIME_KIND in kind:
                        self.Kind.QueryTime = str(kind[const.QUERY_TIME_KIND])
                    if const.CAUSE_CODE_KIND in kind:
                        self.Kind.CauseCode = str(kind[const.CAUSE_CODE_KIND])
                    if const.CPS_METRIC_KIND in kind:
                        self.Kind.CpsMetric = str(kind[const.CPS_METRIC_KIND])

            args = self.parsearguments()
            if args is not None:
                #Common Arguments
                if args.input is not None:
                    self.InputFolder = args.input
                if args.output is not None:
                    self.OutputFolder = args.output
                if args.output_pattern is not None:
                    self.OutputFilePattern = args.output_pattern
                if args.interval is not None:
                    self.FileCreateInterval = args.interval
                if args.query_time_interval is not None:
                    self.QueryTimeInterval = args.query_time_interval
                if args.cause_code_interval is not None:
                    self.CauseCodeInterval = args.cause_code_interval
                if args.cps_metric_interval is not None:
                    self.CPSMetricInterval = args.cps_metric_interval
                if args.node is not None:
                    self.NodeName = args.node
                if args.log_folder is not None:
                    self.LogFolder = args.log_folder
                #On-Demand Process arguments
                if args.ondemand is not None:
                    self.OnDemandProcess = args.ondemand
                if args.input_pattern is not None:
                    self.InputFilePattern = args.input_pattern
                if args.output_types is not None:
                    #types = args.output_types.pop()
                    if len(args.output_types) > 0:
                        for t in args.output_types:
                            types = list(filter(lambda x: x.strip().lower(), t))
                            #if len(t) > 1:
                            #    types = list(filter(lambda x: x, t))
                            for typ in types:
                                ot = "".join(typ)
                                if ot == const.OT_QUERY and const.OT_QUERY not in self.OutputTypes:
                                    self.OutputTypes.append(const.OT_QUERY)
                                elif ot == const.OT_CODE and const.OT_CODE not in self.OutputTypes:
                                    self.OutputTypes.append(const.OT_CODE)
                                elif ot == const.OT_CPS and const.OT_CPS not in self.OutputTypes:
                                    self.OutputTypes.append(const.OT_CPS)
        except Exception as ex:
            err = str(ex)
            return f"Config file could not read: {ex}"
        finally:
            if fd is not None:
                fd.close()

    def validate(self):
        error = None
        if self.OnDemandProcess:
            if os.path.isfile(self.InputFolder):
                error = "Watch folder must be directory"
            elif not os.path.exists(self.InputFolder):
                error = "Watch folder does not exists"
        else:
            if os.path.isfile(self.InputFolder):
                error = "Watch folder must be directory"
            elif not os.path.exists(self.InputFolder):
                error = "Watch folder does not exists"

        if os.path.isfile(self.OutputFolder):
            error = "Output folder must be directory"
        elif not os.path.exists(self.OutputFolder):
            error = "Output folder does not exists"
        if self.OutputTypes is not None and len(self.OutputTypes) > 0:
            unsupportedtypes = list(filter(lambda x: x != const.OT_QUERY and x != const.OT_CODE and x != const.OT_CPS, self.OutputTypes))
            if unsupportedtypes is not None and len(unsupportedtypes) > 0:
                error = "Unsupported output type" + ", ".join(unsupportedtypes)

        return error

    def output_type(self, types):
        def find_type(ot):
            if ot is None:
                return const.OUTPUT_TYPES
            for key, item in enumerate([choice.lower() for choice in types]):
                if ot.lower() == item:
                    return types[key]
            else:
                return ot
        return find_type

    def parsearguments(self):
        parser = ArgumentParser(
            usage="python %(prog)scdraggregator.py [PROCESS OPTIONS]|[TEST OPTIONS]",
            description="Examine over cdr files and generate aggregated data ",
            allow_abbrev=False,
            epilog= """
                Examples:
                1.  The following example filters .cdr files which is contains '15' in name in the /home/input/test folder and generate output files for "query times" and "reason code" to /home/output.
                    Once all files processed then program terminates 
                    
                    python ./%(prog)s --input=/home/input/test --output=/home/output --input-pattern=*15*.cdr --output-types Query Code 
                    
                    
                2.  The following example watch working directory and  generate output files to /home/output folder continously per 15 minutes. Node name will be set as "Odine Test Node"
                    Once all files processed then program waits new files until to terminates by user
                    
                    python ./%(prog)s --output=/home/output --interval=15 --node="Odine Test Node"
                    
                    
                3.  Program watch working directory and waits the new files with default values. Then generates output files to same directory or all output types
                    Once all files processed then program waits new files until to terminates by user
                
                    python ./%(prog)s
                    
            """
        )
        common_options_group = parser.add_argument_group("COMMON OPTIONS")
        process_options_group = parser.add_argument_group("PROCESS OPTIONS")
        test_options_group = parser.add_argument_group("TEST OPTIONS")

        process_options_group.add_argument("-i", "--input", metavar="", dest="input", type=str, required=False,
                                           help="Indicates the folder which will be used as source to get cdr files. Default: working directory")
        process_options_group.add_argument("-o", "--output", metavar="", dest="output", type=str, required=False,
                                           help="Indicates which folder will used to store the files contains aggregated data, Default: working directory")

        process_options_group.add_argument("-op", "--output-pattern", metavar="", dest="output_pattern", type=str, required=False,
                                           help="Template of output file name. [NODE], [TIMESTAMP] and [TYPE] variables will be replaced. Default: [NODE]_[TIMESTAMP]_[TYPE].csv")
        process_options_group.add_argument("--interval", metavar="", dest="interval", type=int, required=False,
                                          help="Indicates how many minutes the files will be created in an hour. Default: 15 minutes")
        process_options_group.add_argument("-qt", "--query-time-interval", metavar="", dest="query_time_interval",
                                           type=int, required=False,
                                           help="Indicates how many minute intervals the data will be grouped in each file for query times. Default: 5 minutes")
        process_options_group.add_argument("-cc", "--cause-code-interval", metavar="", dest="cause_code_interval",
                                           type=int, required=False,
                                           help="Indicates how many minute intervals the data will be grouped in each file for cause codes. Default: 5 minutes")
        process_options_group.add_argument("-cps", "--cps-metric-interval", metavar="", dest="cps_metric_interval",
                                           type=int, required=False,
                                           help="Indicates how many minute intervals the data will be grouped in each file for cps metrics. Default: 1 minutes")
        process_options_group.add_argument("--node", metavar="", dest="node", type=str, required=False,
                                           help="Node name which is used for replace [NODE] variable. Default: current machine name")
        test_options_group.add_argument("-s", "--source", metavar="", dest="input", type=str, required=False,
                                        help="Indicates the folder which will be used as source to get cdr files. Default: working directory")
        test_options_group.add_argument("-ip", "--input-pattern", metavar="", dest="input_pattern", type=str, required=False,
                                        help="Accept pattern for input file names. Wildcards (* or ?) can be used. Available only for on-demand process. Default: *.cdr")

        test_options_group.add_argument("-ot", "--output-types", metavar="", dest="output_types", required=False, action="append", nargs="*", type=self.output_type(const.OUTPUT_TYPES),
                                        help="Indicates how many minutes the files will be created in an hour. Available only for on-demand process. Output generate for all types if omitted")

        common_options_group.add_argument("-l", "--log-folder", metavar="", dest="log_folder", type=str,
                                          required=False,
                                          help="Folder where log files will be stored. Default: log folder in current directory ")
        common_options_group.add_argument("-qts", "--query-time-suffix", metavar="", dest="query_time_suffix", type=str,
                                          required=False,
                                          help="Suffix which will be replaced with [TYPE] variable on file name. Default: querytimes")
        common_options_group.add_argument("-ccs", "--cause-code-suffix", metavar="", dest="cause_code_suffix", type=str,
                                          required=False,
                                          help="Suffix which will be replaced with [TYPE] variable on file name. Default: causecodes")
        common_options_group.add_argument("-cpss", "--cps-metric-suffix", metavar="", dest="cps_metric_suffix",
                                          type=str, required=False,
                                          help="Suffix which will be replaced with [TYPE] variable on file name. Default: cpsmetrics")

        common_options_group.add_argument("-t", "--test", dest="ondemand", required=False, action="store_true",
                                          help="Specifies working mode. Watching mode enabled if omitted. "
                                               "If this option set, program will be terminated all files in folder processed. Otherwise program will be waits to new files until to terminated by use")
        return parser.parse_args()

#include "taxi.hpp"

int main(int argc, char* argv[]) {

    int option = 0;
    int index = 0;
    string data_path = "../data/def";
    int maxServingDelay = 0;
    int servinSpeedFactor = -1;
    int parallelism = 1;
    int executionTime = 60;
    int i = 0;
    int rate = 0;
    total_rides = 0;
    sink_zero_processed = 0;

    opterr = 1;
    if (argc == 11) {
        while ((option = getopt_long(argc, argv, "s:p:c:e:r:", long_opts, &index)) != -1) {
            switch (option) {
                case 's': parallelism = atoi(optarg);
                    break;
                case 'p': parallelism = atoi(optarg);
                    break;
                case 'c': parallelism = atoi(optarg);
                    break;
                case 'e': parallelism = atoi(optarg);
                    break;
                case 'r': rate = atoi(optarg);
                    break;
                default:
                    exit(EXIT_SUCCESS);
            }
        }
    } else if (argc == 5) {
        while ((option = getopt_long(argc, argv, "n:r:", long_opts, &index)) != -1) {
            switch (option) {
                case 'n':
                    parallelism = atoi(optarg);
                    break;
                case 'r': rate = atoi(optarg);
                    break;
                default:
                    exit(EXIT_SUCCESS);
            }
        }
    } else if (argc == 2) {
        while ((getopt_long(argc, argv, "h", long_opts, &index)) != -1) {
            exit(EXIT_SUCCESS);
        }
    } else {
        exit(EXIT_SUCCESS);
    }

    //parse file
    string line;
    ifstream myfile(data_path);
    int countCharacteres = 0;
    int countlines = 0;
    vector<string> rideLines;
    if (myfile.is_open())
    {
        while ( getline (myfile,line)) // && countlines <= 10000
        {
            rideLines.push_back(line);
            countCharacteres += line.size();
            countlines++;
        }
        myfile.close();
    }
    else cout << "Unable to open file";


    unsigned long app_start_time = current_time_usecs();

    /// create the nodes
    Source_Functor source_functor(rideLines, rate, app_start_time);
    Source source = Source_Builder(source_functor)
            .withParallelism(parallelism)
            .withName("Source Parcer")
            .build();

    Filter_Functor filterFunctor;
    Filter filter = Filter_Builder(filterFunctor)
            .withParallelism(parallelism)
            .withName("Filter")
            .build();

    FlatMap_Functor flatMapFunctor;
    FlatMap flatMap = FlatMap_Builder(flatMapFunctor)
            .withParallelism(parallelism)
            .withName("FlatMapper")
            .build();

    Pane_Farm pf = PaneFarm_Builder(F, G)
            .withCBWindows(100, 1)
            .withParallelism(parallelism, parallelism)
            .withName("PaneFarm")
            .prepare4Nesting()
            .build();

    Key_Farm keyFarm = KeyFarm_Builder(pf)
            .withParallelism(parallelism)
            .withName("KeyFarm")
            .build();

    FlatMap_Functor2 flatMapFunctor2;
    FlatMap flatMap2 = FlatMap_Builder(flatMapFunctor2)
            .withParallelism(parallelism)
            .withName("FlatMapper2")
            .build();

    Sink_Functor sink_functor(0);
    Sink sink = Sink_Builder(sink_functor)
            .withParallelism(parallelism)
            .withName("Sink")
            .build();

    PipeGraph env("Taxi Ride App");
    MultiPipe &pipe = env.add_source(source);
    pipe.add(filter);
    pipe.add(flatMap);
    pipe.add(keyFarm);
    pipe.add(flatMap2);
    pipe.chain_sink(sink);
    env.run();

    volatile unsigned long start_time_main_usecs = current_time_usecs();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double tot_average_latency = 1 / (parallelism - sink_zero_processed);

    cout << "Total de corridas computadas: " << total_rides << endl;
    cout << "Finish!" << endl;
    return 0;
}
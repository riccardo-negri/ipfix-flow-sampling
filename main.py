"""Run flow sampling algorithms in parallel consuming from a Kafka topic and producing on a different Kafka topic."""

from multiprocessing import Process, Value
from time import sleep

from dotenv import load_dotenv
from tabulate import tabulate

from logger import logger
from methods.method import Method
from methods.smart_sampling import (
    Features,
    MultifactorParallelSampling,
    MultifactorSmartSampling,
)


def start_method(method: Method, tot_counter: int, sampled_counter: int) -> list[Process]:
    """Start 4 instances of the method, each consumes and produces independently, but is part of the same group."""
    processes = []
    processes += [
        Process(
            target=method.start,
            args=(tot_counter, sampled_counter),
            name="method",
        )
        for _ in range(8)
    ]

    for p in processes:
        p.start()

    return processes


if __name__ == "__main__":
    # load environment variables from .env file
    load_dotenv()

    methods = [
        MultifactorParallelSampling(
            features = Features(
                random_prob = 1,
            ),
            writer_id="ground-truth",
        ),
        MultifactorParallelSampling(
            features = Features(
                random_prob = 0.8,
                renormilize_probs=True,
            ),
            writer_id="80%-prob",
        ),
        MultifactorSmartSampling(
            features = Features(
                bytes={"threshold": 4000000, "weight": 1},
            ),
            writer_id="mf-smart-4MB",
        ),
        MultifactorSmartSampling(
            features = Features(
                packets={"threshold": 4096, "weight": 1},
            ),
            writer_id="mf-smart-4096p",
        ),
        MultifactorSmartSampling(
            features = Features(
                packets={"threshold": 8192, "weight": 1},
                bytes={"threshold": 8000000, "weight": 1},
            ),
            writer_id="mf-smart-8192p-8MB",
        ),
        MultifactorParallelSampling(
            features = Features(
                packets={"threshold": 6144, "weight": 1},
                src_ports={"values": ["53", "2123", "5060"], "weight": 0.25},
                dst_ports={"values": ["53", "2123", "5060"], "weight": 0.25},
                dropped_status={"threshold": 1, "weight": 1},
                renormilize_probs=True,
            ),
            writer_id="mf-parallel-6144p-ports25%-drop-norm",
        ),
        MultifactorParallelSampling(
            features = Features(
                packets={"threshold": 8192, "weight": 1},
                random_prob = 0.20,
                dropped_status={"threshold": 1, "weight": 1},
                renormilize_probs=True,
            ),
            writer_id="mf-parallel-8192p-20%-drop-norm",
        ),
        MultifactorParallelSampling(
            features = Features(
                packets={"threshold": 8192, "weight": 1},
                random_prob = 0.10,
                src_ports={"values": ["53", "2123", "5060"], "weight": 0.50},
                dst_ports={"values": ["53", "2123", "5060"], "weight": 0.50},
                dropped_status={"threshold": 1, "weight": 1},
                renormilize_probs=True,
            ),
            writer_id="mf-parallel-8192p-10%-ports50%-drop-norm",
        ),
        MultifactorParallelSampling(
            features = Features(
                packets={"threshold": 8192, "weight": 1},
                random_prob = 0.05,
                src_ports={"values": ["53", "2123", "5060"], "weight": 0.50},
                dst_ports={"values": ["53", "2123", "5060"], "weight": 0.50},
                dropped_status={"threshold": 1, "weight": 1},
                syn_flag = {"threshold": 1, "weight": 0.25},
                renormilize_probs=True,
            ),
            writer_id="mf-parallel-8192p-5%-ports50%-drop-syn25%-norm",
        ),
    ]

    # initialize as many counters to zero as the number of methods
    tot_counters = {}
    sampled_counters = {}
    method_processes = {}
    for method in methods:
        tot_counters[method.writer_id] = Value("i", 0)
        sampled_counters[method.writer_id] = Value("i", 0)
        method_processes[method.writer_id] = []

    logger.info("Starting consumers and methods")

    processes = []
    for m in methods:
        ps = start_method(m, tot_counters[m.writer_id], sampled_counters[m.writer_id])
        processes += ps
        method_processes[m.writer_id] = ps

    logger.info("Started consumers and methods")

    prev_tot_counters = {k: 0 for k in tot_counters.keys()}
    total_messages = 0
    try:
        while True:
            interval = 5
            sleep(interval)

            sampled_data = [
                [
                    method.writer_id,
                    sampled_counters[method.writer_id].value,
                    tot_counters[method.writer_id].value,
                ]
                for method in methods
            ]

            total_messages = sum(
                [tot_counters[k].value - prev_tot_counters[k] for k in tot_counters.keys()]
            ) // interval

            messages_per_method = [
                [
                    method.writer_id,
                    (tot_counters[method.writer_id].value
                    - prev_tot_counters[method.writer_id])//interval,
                ]
                for method in methods
            ]

            running_processes = [
                [
                    method.writer_id,
                    sum(p.is_alive() for p in method_processes[method.writer_id])
                ]
                for method in methods
            ]

            # Update previous counters
            for method in methods:
                prev_tot_counters[method.writer_id] = tot_counters[method.writer_id].value

            # Print all stats in a tabular format
            print("\033c", end="")  # Clear the screen
            print("\nCounters:")
            print(
                tabulate(
                    sampled_data,
                    headers=["Writer ID", "Sampled Flows", "Total Flows"],
                    tablefmt="grid",
                )
            )
            print("\nMessages per method:")
            print(
                tabulate(
                    messages_per_method,
                    headers=["Writer ID", "Flows/second"],
                    tablefmt="grid",
                )
            )
            print("\nRunning Processes per method:")
            print(
                tabulate(
                    running_processes,
                    headers=["Writer ID", "Running Processes"],
                    tablefmt="grid",
                )
            )
            print(f"\nTotal: {total_messages} flows/second")
    except KeyboardInterrupt:
        logger.info("Interrupted by user, stopping processes")
        for p in processes:
            p.terminate()
            p.join()
        logger.info("Stopped processes")
        logger.info("Exiting")
        exit(0)

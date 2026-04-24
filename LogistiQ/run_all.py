import argparse
import asyncio
import os
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _python_cmd() -> str:
	return sys.executable or "python"


def _build_commands(count: int, interval: float) -> list[tuple[str, list[str]]]:
	python = _python_cmd()
	return [
		(
			"api",
			[
				python,
				"-m",
				"uvicorn",
				"api.server:app",
				"--host",
				"127.0.0.1",
				"--port",
				"8000",
			],
		),
		(
			"simulator",
			[
				python,
				str(ROOT / "simulator" / "data_generator.py"),
				"--count",
				str(count),
				"--interval",
				str(interval),
			],
		),
		(
			"scout",
			[python, str(ROOT / "agents" / "scout.py")],
		),
		(
			"router",
			[python, str(ROOT / "agents" / "router.py")],
		),
		(
			"audit",
			[python, str(ROOT / "agents" / "audit.py")],
		),
		(
			"sentinel",
			[python, str(ROOT / "agents" / "sentinel.py")],
		),
		(
			"arbiter",
			[python, str(ROOT / "agents" / "arbiter.py")],
		),
		(
			"commander",
			[python, str(ROOT / "agents" / "commander.py")],
		),
	]


async def _stream_output(name: str, stream: asyncio.StreamReader | None) -> None:
	if stream is None:
		return
	while True:
		line = await stream.readline()
		if not line:
			break
		print(f"[{name}] {line.decode(errors='replace').rstrip()}")


async def _start_process(name: str, command: list[str]) -> asyncio.subprocess.Process:
	return await asyncio.create_subprocess_exec(
		*command,
		cwd=str(ROOT),
		stdout=asyncio.subprocess.PIPE,
		stderr=asyncio.subprocess.PIPE,
		env=os.environ.copy(),
	)


async def run_all(count: int, interval: float) -> int:
	process_specs = _build_commands(count, interval)
	processes: list[tuple[str, asyncio.subprocess.Process]] = []
	output_tasks: list[asyncio.Task[None]] = []

	for name, command in process_specs:
		process = await _start_process(name, command)
		processes.append((name, process))
		output_tasks.append(asyncio.create_task(_stream_output(name, process.stdout)))
		output_tasks.append(asyncio.create_task(_stream_output(name, process.stderr)))

	try:
		results = await asyncio.gather(*(process.wait() for _, process in processes))
		return max(results) if results else 0
	except asyncio.CancelledError:
		for _, process in processes:
			if process.returncode is None:
				process.terminate()
		raise
	finally:
		for _, process in processes:
			if process.returncode is None:
				process.terminate()
		await asyncio.gather(*output_tasks, return_exceptions=True)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Run the full LogistiQ agent pipeline: simulator, API, Scout, Router, Audit, Sentinel, Arbiter, and Commander.")
	parser.add_argument("--count", type=int, default=1, help="Number of simulator events to emit. Use 0 for continuous streaming.")
	parser.add_argument("--interval", type=float, default=2.0, help="Seconds between simulated events.")
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	if hasattr(signal, "SIGINT"):
		signal.signal(signal.SIGINT, signal.SIG_DFL)
	try:
		exit_code = asyncio.run(run_all(args.count, args.interval))
	except KeyboardInterrupt:
		exit_code = 130
	raise SystemExit(exit_code)


if __name__ == "__main__":
	main()
import subprocess
import sys
import os

from config import RESOURCES_DIR, EXPERIMENTS_DIR


def main():
    # auto-create all necessary directories
    os.makedirs(RESOURCES_DIR, exist_ok=True)
    os.makedirs(EXPERIMENTS_DIR, exist_ok=True)

    if len(sys.argv) < 2:
        print("Использование: python main.py [stage_one | stage_two]")
        sys.exit(1)

    stage = sys.argv[1]

    if stage == "stage_one":
        subprocess.Popen(["python", "stage_one.py"], env=dict(os.environ, PATH="path"))
    elif stage == "stage_two":
        subprocess.Popen(["python", "stage_two.py"], env=dict(os.environ, PATH="path"))
    else:
        print(f"Неизвестный этап: {stage}. Доступные этапы: stage_one, stage_two")
        sys.exit(1)


if __name__ == "__main__":
    main()

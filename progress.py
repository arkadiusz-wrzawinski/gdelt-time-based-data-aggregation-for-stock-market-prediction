from rich.progress import (Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TaskProgressColumn,
                           TextColumn, ProgressColumn, Task, MofNCompleteColumn)
from rich.text import Text


class SpeedColumn(ProgressColumn):
    def render(self, task: "Task") -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        return Text(f"{int(speed)}/s", style="progress.data.speed")

def make_progress() -> Progress:
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        SpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=True,
    )
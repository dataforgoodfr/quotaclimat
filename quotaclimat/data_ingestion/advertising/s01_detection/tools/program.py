import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass
class Show:
    channel_name: str
    # These start and end are relative to the 0 of strptime: Show.start_of_reference_week()
    start: datetime
    end: datetime
    program_name: str = ""
    # Validity of the program grid this show belongs to (inclusive), None = no bound
    grid_start: date | None = None
    grid_end: date | None = None

    # Return exact start and end datetime of the show for a given week, by adding the relative start and end to the start of the week.
    def for_week(self, week_start_date: datetime) -> tuple[datetime, datetime]:
        absolute_start = Show.start_of_reference_week()
        return (
            week_start_date + (self.start - absolute_start),
            week_start_date + (self.end - absolute_start),
        )

    # True if the program grid of this show is valid on the day the show airs during the given week
    def is_on_air_for_week(self, week_start_date: datetime) -> bool:
        show_date = self.for_week(week_start_date)[0].date()
        if self.grid_start and show_date < self.grid_start:
            return False
        if self.grid_end and show_date > self.grid_end:
            return False
        return True

    @classmethod
    def start_of_reference_week(cls):
        return datetime.strptime("", "")

    @classmethod
    def end_of_reference_week(cls):
        return cls.start_of_reference_week() + timedelta(days=7)


def get_program() -> list[Show]:
    program_metadata_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "..",
        "postgres",
        "program_metadata.json",
    )
    with open(program_metadata_path, "r", encoding="utf-8") as f:
        for program in json.load(f):
            start = datetime.strptime(
                str(program["weekday"]) + " " + program["start"], "%d %H:%M"
            )
            end = datetime.strptime(
                str(program["weekday"]) + " " + program["end"], "%d %H:%M"
            )
            # Shows crossing midnight (e.g. 23:45 -> 00:45) end the next day
            if end <= start:
                end += timedelta(days=1)
            yield Show(
                channel_name=program["channel_name"],
                start=start,
                end=end,
                program_name=program.get("program_name") or "",
                grid_start=_parse_grid_date(program.get("program_grid_start")),
                grid_end=_parse_grid_date(program.get("program_grid_end")),
            )


def _parse_grid_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def get_channel_program(
    channel_name: str, week_start_date: datetime | None = None
) -> list[Show]:
    # With week_start_date, only keep shows whose program grid is valid during that week:
    # program_metadata.json keeps the old grids of a channel next to the current one.
    return [
        program
        for program in get_program()
        if program.channel_name == channel_name
        and (week_start_date is None or program.is_on_air_for_week(week_start_date))
    ]


def merge_shows_in_program(program: list[Show]) -> list[Show]:
    # This function merge consecutive shows in the same channel into a single show.
    # For example, if there are two shows on TF1 on Monday from 20:00 to 21:00 and from 21:00 to 22:00, they would be merged into a single show from 20:00 to 22:00.
    if not program:
        return []

    # Sort shows by start time
    program.sort(key=lambda show: show.start)
    merged_program = [program[0]]
    for show in program[1:]:
        last_show = merged_program[-1]
        if show.channel_name == last_show.channel_name and show.start <= last_show.end:
            # Merge shows by extending the end time of the last show
            merged_program[-1] = Show(
                channel_name=last_show.channel_name,
                start=last_show.start,
                end=max(last_show.end, show.end),
            )
        else:
            merged_program.append(show)

    return merged_program


def extend_program_by(program: list[Show], delta: timedelta) -> list[Show]:
    # This function extends the start and end time of each show by a given timedelta.

    start_of_reference_week = Show.start_of_reference_week()
    end_of_reference_week = Show.end_of_reference_week()

    extended_program = []
    for show in program:
        extended_show = Show(
            channel_name=show.channel_name,
            start=show.start - delta,
            end=show.end + delta,
        )

        # If the period now start before the week, we add this part at the end of the week, and we truncate the start to the start of the week
        if extended_show.start < start_of_reference_week:
            extended_program.append(
                Show(
                    channel_name=show.channel_name,
                    start=extended_show.start + timedelta(days=7),
                    end=end_of_reference_week,
                )
            )
            extended_show.start = start_of_reference_week

        # Same for the end of the week
        if extended_show.end > end_of_reference_week:
            extended_program.append(
                Show(
                    channel_name=show.channel_name,
                    start=start_of_reference_week,
                    end=extended_show.end - timedelta(days=7),
                )
            )
            extended_show.end = end_of_reference_week

        extended_program.append(extended_show)

    # In the end we merge all overlaps
    return merge_shows_in_program(extended_program)


if __name__ == "__main__":
    program = get_channel_program("tf1")
    new_program = extend_program_by(program, timedelta(minutes=15))
    print(new_program)

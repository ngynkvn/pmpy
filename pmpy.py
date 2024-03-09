import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DFG:
    pass


CASE_KEY = "@@case_id"
ACTIVITY_KEY = "@@activity"
TIME_KEY = "@@timestamp"


def from_pandas(
    df: pd.DataFrame, case_id=CASE_KEY, activity=ACTIVITY_KEY, timestamp=TIME_KEY
) -> pd.DataFrame:
    df = df.copy()
    df[CASE_KEY] = df[case_id]
    df[ACTIVITY_KEY] = df[activity]
    df[TIME_KEY] = df[timestamp]
    return df


def dfg(df: pd.DataFrame, case_id=CASE_KEY, activity=ACTIVITY_KEY, timestamp=TIME_KEY):
    view = df[[case_id, activity, timestamp]]
    shifted = view.shift(-1)
    shifted.columns = [f"{s}_2" for s in shifted.columns]

    joined = pd.concat([view, shifted], axis=1)
    joined = joined[joined[case_id] == joined[case_id + "_2"]]

    joined.to_csv("test.csv")

    count = joined.groupby([activity, activity + "_2"]).size()

    display(count)

    return count.to_dict()


def start_activities(
    df: pd.DataFrame, case_id=CASE_KEY, activity=ACTIVITY_KEY, timestamp=TIME_KEY
):
    view = df[[case_id, activity, timestamp]]
    return view.groupby(case_id).first()[activity].value_counts()


def end_activities(
    df: pd.DataFrame, case_id=CASE_KEY, activity=ACTIVITY_KEY, timestamp=TIME_KEY
):
    view = df[[case_id, activity, timestamp]]
    return view.groupby(case_id).last()[activity].value_counts()


if __name__ == "__main__":
    import pandas as pd
    from IPython.display import display

    events = pd.read_csv("./data/flight_event_log.csv")
    events = from_pandas(
        events, case_id="Flight", activity="Activity", timestamp="Timestamp"
    )
    display("\n".join(map(str, dfg(events).items())))
    print()
    display(start_activities(events))
    display(end_activities(events))

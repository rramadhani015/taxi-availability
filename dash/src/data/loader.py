import pandas as pd

class DataSchema:
    SELLING_PRICE="selling_price"
    AVAILABILITY="availability"
    COLOR="color"
    CATEGORY="category"
    AVERAGE_RATING="average_rating"
    REVIEWS_COUNT="reviews_count"


def load_raw_adidas_data(path: str) -> pd.DataFrame:
    #load csv file
    data = pd.read_csv(
        path, dtype={
            DataSchema.SELLING_PRICE: float,
            DataSchema.AVAILABILITY: str,
            DataSchema.COLOR: str,
            DataSchema.CATEGORY: str,
            DataSchema.AVERAGE_RATING: float,
            DataSchema.REVIEWS_COUNT: int
        }
    )
    return data
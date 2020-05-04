def dataframeToLocalTsv(
    dataframe,
    path
):
    dataframe.to_csv(
        path,
        mode = 'w',
        sep = '\t',
        index = False,
        header = False
    )

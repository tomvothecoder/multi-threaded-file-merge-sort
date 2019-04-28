import os

import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
from scipy import stats


def input_to_df(file_name):
    """
    Convert the input CSV to a dataframe, while dropping rows with null

    Args:

    Returns:
        csv_list (list)

    """
    df = pd.read_csv(os.path.join('input_dataset/', file_name), sep=";")
    df = df[["Country", "Unemployment rate(%)", "Death rate(deaths/1000 population)"]].dropna()
    csv_list = df.values[1:].tolist()
    return csv_list


def output_csv(df, out_file):
    """
    Output the
    Args:
        df:
        out_file:

    Returns:

    """
    output = pd.DataFrame(df)
    output.columns = ["Country", "Unemployment rate(%)", "Death rate(deaths/1000 population)"]
    output.to_csv('output_dataset/' + out_file, index=False)


def path_file(input_file):
    path = os.path.abspath(input_file)
    directory = os.path.dirname(path)

    return directory


def data_to_plotly(x):
    """
    Source: https://plot.ly/scikit-learn/plot-ols/
    Args:
        x:

    Returns:

    """
    k = []

    for i in range(0, len(x)):
        k.append(x[i][0])

    return k


def plot_output(outfile_name):
    """
    Plot the output dataset using Plotly. Linear regression line is
    calculated Scipy linregress function.
    Args:
        outfile_name:

    Returns:

    """
    df = pd.read_csv(os.path.join('output_dataset/', outfile_name))

    x = df['Unemployment rate(%)']
    y = df['Death rate(deaths/1000 population)']

    # Generated linear fit
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    line = slope * x + intercept

    p1 = go.Scatter(x=x,
                    y=y,
                    mode='markers',
                    name='Value')

    p2 = go.Scatter(x=x,
                    y=line,
                    mode='lines',
                    name='Fit'
                    )

    layout = go.Layout(title='Death Rates per Country in relation to Pop. Unemployment %',
                       xaxis={'title': x.name},
                       yaxis={'title': y.name},
                       )
    fig = dict(data=[p1, p2], layout=layout)

    pio.write_image(fig, f'output_dataset/{outfile_name}.png')

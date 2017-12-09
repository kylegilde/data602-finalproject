
    len(MTA_weather_df)
    ridership_data['Zip Code - 3 Digits'].value_counts()
    ridership_data['Zip Code - 3 Digits'].value_counts()
    type(ridership_data['Date'][0])

    ridership_data['Date'].describe()
    weather_df['Date'].describe()

    lst = list(set(rdates) | set(wdates))
    len(lst)
    rdates = ridership_data['Date'].unique()
    wdates = weather_df['Date'].unique()
    len(weather_df)
    ridership_data['Zip Code - 3 Digits'].value_counts()
    ridership_data['Zip Code - 3 Digits'].isnull().sum()

    df = ridership_data.merge(weather_df, how='left', on=['Zip Code - 3 Digits', 'Date'])

    pd.to_csv()

    b = pd.DataFrame()
    b = weather_df[weather_df['Date'] >= '2015-01-01T00:00:00.000000000']
    c = b.groupby(['Zip Code - 3 Digits', 'Date']).size()
    len(c)
    3 * 365




    b = b[[b['Date'] > '2015-01-01T00:00:00.000000000']]

    weather_df['Date'].unique()
    pd.DataFrame(b).to_csv('test')

    weather_df[['Zip Code - 3 Digits', 'Date']].groupby(['Zip Code - 3 Digits', 'Date']).size()

    weather_df['Zip Code - 3 Digits'].value_counts()
    weather_df['Zip Code - 3 Digits'].value_counts()

    weather_df['Zip Code']

    weather_df['Zip Code - 3 Digits'].value_counts()


    mstations = MTA_weather_df['Station'].unique()
    rstations = ridership_data['Station'].unique()

    MTA_weather_df['Station'].isin(ridership_data['Station'])
    MTA_weather_df['Station'].value_counts()
    ridership_data['Station'].value_counts()


    a = ridership_data['Station'] + ridership_data['Zip Code - 3 Digits']
    a.value_counts()
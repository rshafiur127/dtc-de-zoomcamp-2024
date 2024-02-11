if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):    
    print("Rows with zero passenger:" , data['passenger_count'].isin([0]).sum())
    print("Rows with zero trip:" , data['trip_distance'].isin([0]).sum())

    # (df.points > 20) | (df.assists == 9)
    # return data[(data['passenger_count'] > 0 | data['trip_distance'] > 0.0)]
    
    #data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    # lpep_pickup_date by converting lpep_pickup_datetime

    #df[lpep_pickup_date] = pd.to_datetime(data[lpep_pickup_datetime])
    #data['lpep_pickup_datetime'] = data['lpep_pickup_datetime'].dt.date
    #data['lpep_pickup_datetime'] = data['lpep_pickup_datetime'].dt.date
    #data['lpep_dropoff_datetime'] = data['lpep_dropoff_datetime'].dt.date


    data = data.rename(columns={"VendorID" : "vendor_id"})

    data.columns = (data.columns
                    .str.replace(' ', '_')
                    .str.lower()
    )
    
    return data

#@test
#def passenger_count(output, *args):
#    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passenger'
#def trip_distance(output, *args):
#    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
#def vendor_id(output, *args):    
#    assert output['VendorID'].isin([output[VendorID].unique()])
    



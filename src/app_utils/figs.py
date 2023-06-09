import pandas as pd
import plotly.express as px
import json

from src.db.db_queries import fetch_all_from_table

def graph_world_electricity_emissions():
    # Fetch data
    emissions_df = fetch_all_from_table('World_electricity_emissions')
    emissions_df['emissions'] *= 1_000 # Convert from kgCO2e/kWh to gCO2e/kWh
    emissions_df['emissions'] = round(emissions_df['emissions'],2)
    
    # Remove outlier
    emissions_df.drop(emissions_df[emissions_df['country_iso_3'] == 'BWA'].index, inplace=True)
    
    # Plot graph
    fig = px.choropleth(emissions_df, 
                        locationmode="ISO-3", 
                        color="emissions",
                        color_continuous_scale="Viridis",
                        locations="country_iso_3",
                        projection="natural earth",
                        width=1200, height=750,
                        # hover_data={'Department': False, 'emissions': ':.2f'},
                        labels={'country_iso_3':'Country','emissions': 'Emissions (gCO2e/kWh)'},
                        title='Average electricity network CO2 eq. emissions per kWh'
                    )
    # Update hover labels
    # fig.update_traces(hovertemplate='%{hovertext}: %{hoverdata[1]:.2f}')
    fig.update_geos(fitbounds="locations", visible=True)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":1})
    fig.update_layout(title_font_size=16, 
                      title_x=0.25,
                      title_y=0.95,
    )

    return fig

def graph_fr_heating_emissions():
    # Fetch data
    emissions_df = fetch_all_from_table('FR_regional_heating_emissions')
    dept_df = fetch_all_from_table('Dim_Departments')
    emissions_df = emissions_df.merge(dept_df, on='dept_id', how='left')
    emissions_df = emissions_df[emissions_df['heat_cycle']=='heat']
    emissions_df = emissions_df[emissions_df['validity_date']=='2020-01-01 00:00:00']
    dept_group = emissions_df.groupby('dept_id').mean().reset_index()  # Simple agg - TODO
    dept_group['emissions'] *= 1_000 # Convert from kgCO2e/kWh to gCO2e/kWh
    dept_group['emissions'] = round(dept_group['emissions'],2)
    
    # Load the GeoJSON file for French departments
    with open('./maps/fr_departements.geojson') as f:
        fr_dept_geojson = json.load(f)

    # Plot graph
    fig = px.choropleth(dept_group, 
                        geojson=fr_dept_geojson, 
                        color="emissions",
                        color_continuous_scale="Viridis",
                        locations="dept_id", 
                        featureidkey="properties.code",
                        projection="mercator",
                        width=1200, height=800,
                        labels={'dept_id':'Department', 'emissions': 'Emissions (gCO2e/kWh)'},
                        title='Average heating network CO2 eq. emissions per kWh'
                    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.update_layout(title_font_size=16, 
                      title_x=0.05,
                      title_y=0.95,
    )
    return fig

def graph_fr_forestry_change(forest_type, tree_type):
    # Fetch data
    forestry_df = fetch_all_from_table('FR_forestry_area')
    dept_df = fetch_all_from_table('Dim_Regions')
    forest_types_df = fetch_all_from_table('Dim_Forest_types')
    tree_types_df = fetch_all_from_table('Dim_Tree_types')
    
    # Merge dimensions
    forestry_df = forestry_df.merge(dept_df, on='region_id', how='left')
    forestry_df = forestry_df.merge(forest_types_df, on='forest_type_id', how='left')
    forestry_df = forestry_df.merge(tree_types_df, on='tree_type_id', how='left')
    
    # Select sub-set as per user request
    forestry_df = forestry_df[forestry_df['forest_type_name'] == forest_type]
    forestry_df = forestry_df[forestry_df['tree_type_name'] == tree_type]
    
    # Load the GeoJSON file for French departments
    with open('./maps/fr_regions.geojson') as f:
        fr_regions_geojson = json.load(f)

    # Plot graph
    fig = px.choropleth(forestry_df, 
                        geojson=fr_regions_geojson, 
                        color="land_loss",
                        color_continuous_scale="Viridis",
                        locations="region_name",
                        featureidkey="properties.nom",
                        projection="mercator",
                        width=1200, height=800,
                        labels={'region_id':'Region', 'land_loss': 'Area change (kgCO2e/ha.yr)'},
                        title='Average emissions (kgCO2e) from forestry area (ha) change per annum.'
                    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.update_layout(title_font_size=16, 
                      title_x=0.05,
                      title_y=0.95,
    )
    return fig
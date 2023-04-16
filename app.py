import streamlit as st

from src.app_utils.figs import graph_world_electricity_emissions
from src.app_utils.figs import graph_fr_heating_emissions
from src.app_utils.figs import graph_fr_forestry_change

def page_config():
    st.set_page_config(
        layout='wide',
        page_title='Emissions study',
        initial_sidebar_state='collapsed',    
    )

def header():
    col1, col2 = st.columns((1,5))
    col2.write("# Emissions Data")

def main():
    page_config()
    header()

    col1, col2, col3 = st.columns((1,3,2))
    dataset = col2.selectbox(label='Datasets', 
                            options=['World electricity emissions',
                                    'France heating network emissions',
                                    'France forestry area change'])
    
    
    # TODO: Match-case on selectbox rather than else-if once more datasets are available    
    if dataset == 'World electricity emissions':
        fig = graph_world_electricity_emissions()
    elif dataset == 'France heating network emissions':
        fig = graph_fr_heating_emissions()
    elif dataset == 'France forestry area change':
        col1, col2, col3, col4 = st.columns((2,1,1,2))
        forest_type = col2.selectbox(label='Forest type', 
                                    options=['Open forest', 'Closed forest', 'Open and closed forest', 'Total'])
        tree_type = col3.selectbox(label='Tree type', 
                                    options=['Deciduous', 'Coniferous', 'Mixed', 'Total'])
        fig = graph_fr_forestry_change(forest_type, tree_type)
    
    st.write('###') # Vertical space
    with st.container():
        st.plotly_chart(fig, )

if __name__ == "__main__":
    main()

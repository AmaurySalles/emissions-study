import streamlit as st

from src.app_utils.figs import graph_world_electricity_emissions
from src.app_utils.figs import graph_fr_heating_emissions

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
                                    'France heating network emissions'])
    
    
    # TODO: Match-case on selectbox rather than else-if once more datasets are available    
    if dataset == 'World electricity emissions':
        fig = graph_world_electricity_emissions()
    elif dataset == 'France heating network emissions':
        fig = graph_fr_heating_emissions()
    
    st.write('###') # Vertical space
    with st.container():
        st.plotly_chart(fig, )

if __name__ == "__main__":
    main()

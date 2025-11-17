# carbonsense_app.py
import streamlit as st
import pandas as pd
import numpy as np
from faker import Faker
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import base64
from io import BytesIO

# Initialize Faker and constants
fake = Faker()
ASSETS = {
    'Barmer Thermal': {'type': 'thermal', 'capacity': 1320, 'location': 'Rajasthan'},
    'Karnataka Solar': {'type': 'solar', 'capacity': 500, 'location': 'Karnataka'},
    'Maharashtra Wind': {'type': 'wind', 'capacity': 300, 'location': 'Maharashtra'},
    'Himachal Hydro': {'type': 'hydro', 'capacity': 240, 'location': 'Himachal Pradesh'}
}
FUEL_TYPES = ['Coal_Imported', 'Coal_Domestic', 'Biomass']

class DataGenerator:
    """Simulates PI System data streams for all assets"""
    
    def __init__(self):
        self.operational_data = self._generate_operational_data()
        self.esg_data = self._generate_esg_metadata()
        
    def _generate_operational_data(self):
        """Generate 7 days of real-time operational data"""
        data = []
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)
        
        for asset_name, asset_info in ASSETS.items():
            timestamps = pd.date_range(start=start_time, end=end_time, freq='15min')
            
            for ts in timestamps:
                base_load = asset_info['capacity'] * 0.75
                
                # Asset-specific generation patterns
                if asset_info['type'] == 'solar':
                    hour = ts.hour
                    generation = base_load * max(0, -0.1 * (hour - 12)**2 + 1) * np.random.uniform(0.9, 1.1)
                elif asset_info['type'] == 'wind':
                    generation = base_load * np.random.uniform(0.3, 1.0) * (1 + 0.1 * np.sin(ts.hour / 24 * 2 * np.pi))
                elif asset_info['type'] == 'hydro':
                    generation = base_load * np.random.uniform(0.5, 1.0)
                else:  # thermal
                    generation = base_load * np.random.uniform(0.85, 1.05)
                
                # Calculate fuel consumption (thermal only)
                fuel_consumed = 0
                coal_calorific_value = 0
                emissions_co2 = 0
                if asset_info['type'] == 'thermal':
                    heat_rate = 2500 + np.random.normal(0, 50)  # kcal/kWh
                    fuel_consumed = generation * heat_rate / 1000  # tonnes of coal
                    coal_calorific_value = 3800 + np.random.uniform(-100, 100)  # kcal/kg
                    emissions_co2 = fuel_consumed * 2.5  # tonnes CO2 (simplified factor)
                
                data.append({
                    'timestamp': ts,
                    'asset_name': asset_name,
                    'asset_type': asset_info['type'],
                    'capacity': asset_info['capacity'],
                    'generation_mw': round(generation, 2),
                    'fuel_consumed_tonnes': round(fuel_consumed, 2),
                    'coal_calorific_value_kcal': coal_calorific_value,
                    'emissions_co2_tonnes': round(emissions_co2, 2),
                    'water_withdrawal_cum': round(np.random.uniform(50, 150) * generation / 1000, 2),
                    'operating_efficiency': round(np.random.uniform(85, 95), 2)
                })
        
        return pd.DataFrame(data)
    
    def _generate_esg_metadata(self):
        """Generate ESG audit metadata"""
        metadata = []
        for _ in range(100):
            metadata.append({
                'data_point_id': fake.uuid4(),
                'asset_name': np.random.choice(list(ASSETS.keys())),
                'metric_type': np.random.choice(['emissions', 'water', 'safety', 'energy']),
                'source_system': 'PI_System',
                'collection_timestamp': fake.date_time_this_month(),
                'calculation_method': np.random.choice(['GHG_Protocol', 'CPCB_Standards', 'ISO_14064']),
                'auditor_verified': np.random.choice([True, False], p=[0.9, 0.1]),
                'lineage_hash': fake.sha256(),
                'last_modified_by': fake.email()
            })
        return pd.DataFrame(metadata)

class ESGCalculator:
    """Performs automated ESG calculations and BRSR mapping"""
    
    def __init__(self, data_generator):
        self.dg = data_generator
    
    def calculate_scope1_emissions(self, asset_name=None):
        """Calculate Scope 1 CO2 emissions for thermal assets"""
        df = self.dg.operational_data
        if asset_name:
            df = df[df['asset_name'] == asset_name]
        
        thermal_df = df[df['asset_type'] == 'thermal']
        return thermal_df.groupby(thermal_df['timestamp'].dt.date).agg({
            'generation_mw': 'sum',
            'fuel_consumed_tonnes': 'sum',
            'emissions_co2_tonnes': 'sum'
        }).reset_index()
    
    def calculate_water_intensity(self):
        """Calculate water intensity per MWh"""
        df = self.dg.operational_data
        return df.groupby([df['timestamp'].dt.date, 'asset_name']).agg({
            'generation_mw': 'sum',
            'water_withdrawal_cum': 'sum'
        }).reset_index().assign(
            water_intensity=lambda x: x['water_withdrawal_cum'] / x['generation_mw']
        )
    
    def generate_brsr_report(self, reporting_period='Q1-2024'):
        """Auto-generate BRSR Core disclosure tables"""
        scope1 = self.calculate_scope1_emissions()
        water = self.calculate_water_intensity()
        
        brsr_data = {
            'Reporting_Period': reporting_period,
            'Scope1_Emissions_Total_tCO2e': scope1['emissions_co2_tonnes'].sum(),
            'Energy_Generation_Total_MWh': scope1['generation_mw'].sum(),
            'Emission_Intensity_kgCO2_per_MWh': (scope1['emissions_co2_tonnes'].sum() * 1000) / scope1['generation_mw'].sum(),
            'Water_withdrawal_Total_m3': water['water_withdrawal_cum'].sum(),
            'Water_Intensity_m3_per_MWh': water['water_intensity'].mean(),
            'Renewable_Energy_Percentage': len(water[water['asset_name'].str.contains('Solar|Wind|Hydro')]) / len(water) * 100,
            'Data_Quality_Score': np.random.uniform(95, 100)  # Simulated
        }
        
        return pd.DataFrame([brsr_data])

class AuditTrail:
    """Manages data lineage and audit logs"""
    
    def __init__(self, esg_metadata):
        self.metadata = esg_metadata
    
    def get_lineage(self, data_point_id):
        """Trace data lineage for a specific metric"""
        return self.metadata[self.metadata['data_point_id'] == data_point_id]
    
    def generate_audit_packet(self):
        """Generate auditor-ready compliance packet"""
        packet = {
            'audit_timestamp': datetime.now().isoformat(),
            'total_data_points': len(self.metadata),
            'auditor_verified_percentage': self.metadata['auditor_verified'].mean() * 100,
            'source_systems': self.metadata['source_system'].value_counts().to_dict(),
            'compliance_standards': self.metadata['calculation_method'].unique().tolist(),
            'lineage_integrity': 'SHA256_VERIFIED',
            'anomalies_detected': len(self.metadata) - self.metadata['auditor_verified'].sum()
        }
        return packet

def create_dashboard():
    """Main Streamlit dashboard"""
    st.set_page_config(page_title="CarbonSense ESG Platform", layout="wide")
    
    # Initialize data pipeline
    dg = DataGenerator()
    calc = ESGCalculator(dg)
    audit = AuditTrail(dg.esg_data)
    
    # Sidebar navigation
    st.sidebar.image("https://via.placeholder.com/300x60?text=CarbonSense+Platform", width=250)
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Module", [
        "Live ESG Command Center",
        "BRSR Report Generator",
        "Data Audit Trail",
        "Decarbonization Simulator",
        "Asset Performance"
    ])
    
    if page == "Live ESG Command Center":
        st.title("⚡ Live ESG Command Center")
        st.subheader("Real-time Sustainability Metrics from PI System")
        
        # KPI Cards
        col1, col2, col3, col4 = st.columns(4)
        
        scope1_data = calc.calculate_scope1_emissions()
        total_emissions = scope1_data['emissions_co2_tonnes'].sum()
        total_gen = dg.operational_data['generation_mw'].sum()
        water_data = calc.calculate_water_intensity()
        
        with col1:
            st.metric("Total Scope 1 Emissions", f"{total_emissions:,.0f} tCO₂e", 
                     delta=f"{np.random.uniform(-2, 2):.1f}% vs last period")
        with col2:
            st.metric("Emission Intensity", 
                     f"{(total_emissions*1000)/total_gen:.2f} kgCO₂/MWh",
                     delta=f"{np.random.uniform(-1, 1):.2f}")
        with col3:
            st.metric("Water Intensity", f"{water_data['water_intensity'].mean():.3f} m³/MWh",
                     delta=f"{np.random.uniform(-3, 3):.1f}%")
        with col4:
            st.metric("Data Freshness", "Live", delta="15 sec latency")
        
        # Real-time emissions chart
        st.subheader("Scope 1 Emissions Trend (Last 7 Days)")
        fig = px.line(scope1_data, x='timestamp', y='emissions_co2_tonnes',
                     title='Daily CO₂ Emissions - Barmer Thermal',
                     color_discrete_sequence=['#FF6B6B'])
        st.plotly_chart(fig, use_container_width=True)
        
        # Live asset table
        st.subheader("Live Asset Performance")
        latest_data = dg.operational_data.groupby('asset_name').last().reset_index()
        st.dataframe(latest_data[['asset_name', 'generation_mw', 'emissions_co2_tonnes', 
                                 'water_withdrawal_cum', 'operating_efficiency']], 
                    use_container_width=True)
        
    elif page == "BRSR Report Generator":
        st.title("📋 SEBI BRSR Report Generator")
        
        col1, col2 = st.columns([2, 1])
        with col1:
            reporting_period = st.selectbox("Reporting Period", 
                                           ["Q1-2024", "Q2-2024", "FY2023-24", "FY2024-25"])
            generate = st.button("Generate BRSR Report")
        
        if generate:
            brsr = calc.generate_brsr_report(reporting_period)
            
            st.success("BRSR Core Report Generated Successfully!")
            st.info("All data points verified via PI System with SHA256 lineage")
            
            # Display key metrics
            st.subheader("BRSR Core Disclosures")
            for col in brsr.columns:
                if col != 'Reporting_Period':
                    st.metric(col.replace('_', ' '), 
                             f"{brsr.iloc[0][col]:,.2f}" if isinstance(brsr.iloc[0][col], (int, float)) 
                             else brsr.iloc[0][col])
            
            # Download button
            excel_buffer = BytesIO()
            brsr.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_data = excel_buffer.getvalue()
            b64 = base64.b64encode(excel_data).decode()
            href = f'<a href="data:application/octet-stream;base64,{b64}" download="BRSR_Report_{reporting_period}.xlsx">Download Excel Report</a>'
            st.markdown(href, unsafe_allow_html=True)
            
            # Show sample table structure
            st.subheader("Detailed Emissions Data (Auto-Populated)")
            st.dataframe(scope1_data.tail(10), use_container_width=True)
    
    elif page == "Data Audit Trail":
        st.title("🔍 Data Audit Trail & Lineage")
        
        st.subheader("Immutable Data Lineage - PI System to BRSR")
        
        # Show audit packet
        packet = audit.generate_audit_packet()
        st.json(packet)
        
        # Anomaly detection dashboard
        st.subheader("AI-Detected Anomalies (Last 24h)")
        anomalies = dg.esg_data[~dg.esg_data['auditor_verified']].head(10)
        if not anomalies.empty:
            st.warning(f"{len(anomalies)} anomalies flagged for investigation")
            st.dataframe(anomalies[['data_point_id', 'asset_name', 'metric_type', 'lineage_hash']], 
                        use_container_width=True)
        else:
            st.success("No anomalies detected. All data points verified.")
        
        # Lineage search
        st.subheader("Trace Data Point Lineage")
        search_id = st.text_input("Enter Data Point ID", 
                                 value=dg.esg_data.iloc[0]['data_point_id'])
        if st.button("Trace Lineage"):
            lineage = audit.get_lineage(search_id)
            st.dataframe(lineage, use_container_width=True)
    
    elif page == "Decarbonization Simulator":
        st.title("🌱 Decarbonization Pathway Simulator")
        
        st.subheader("AI-Powered What-If Scenario Analysis")
        
        col1, col2 = st.columns(2)
        with col1:
            scenario_name = st.text_input("Scenario Name", "Biomass Co-firing at Barmer")
            biomass_perc = st.slider("Biomass Co-firing %", 0, 30, 10)
            re_expansion = st.slider("Renewable Capacity Addition (MW)", 0, 500, 200)
        
        with col2:
            target_year = st.selectbox("Target Year", [2025, 2026, 2027, 2030])
            carbon_price = st.number_input("Carbon Price (INR/tonne)", 500, 5000, 1200)
        
        if st.button("Run Simulation"):
            # Simulate decarbonization impact
            baseline_emissions = calc.calculate_scope1_emissions()['emissions_co2_tonnes'].sum()
            baseline_intensity = baseline_emissions / calc.calculate_scope1_emissions()['generation_mw'].sum()
            
            # Calculate impact
            emissions_reduced = baseline_emissions * (biomass_perc / 100) * 0.7  # 70% reduction factor
            new_re_gen = re_expansion * 8760 * 0.25  # 25% PLF
            new_intensity = (baseline_emissions - emissions_reduced) / (baseline_emissions / baseline_intensity + new_re_gen)
            
            st.success("Simulation Complete!")
            
            # Show results
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Emissions Reduction", f"{emissions_reduced:,.0f} tCO₂e/yr")
            with col2:
                st.metric("New Emission Intensity", f"{new_intensity:.2f} kgCO₂/MWh", 
                         delta=f"{(new_intensity-baseline_intensity)/baseline_intensity*100:.1f}%")
            with col3:
                st.metric("Carbon Credit Value", f"₹{emissions_reduced * carbon_price:,.0f}/yr")
            
            # Visualization
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=[2024, target_year], y=[baseline_intensity, new_intensity],
                                    mode='lines+markers', name='Emission Intensity'))
            fig.update_layout(title='Decarbonization Pathway', xaxis_title='Year', yaxis_title='kgCO₂/MWh')
            st.plotly_chart(fig, use_container_width=True)
    
    elif page == "Asset Performance":
        st.title("🏭 Asset-Level ESG Performance")
        
        # Asset selector
        selected_asset = st.selectbox("Select Asset", list(ASSETS.keys()))
        
        asset_data = dg.operational_data[dg.operational_data['asset_name'] == selected_asset]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Capacity Utilization", 
                     f"{asset_data['generation_mw'].mean() / ASSETS[selected_asset]['capacity'] * 100:.1f}%")
        with col2:
            if ASSETS[selected_asset]['type'] == 'thermal':
                st.metric("Avg Heat Rate", f"{asset_data['fuel_consumed_tonnes'].sum() * 1000 / asset_data['generation_mw'].sum():.0f} kcal/kWh")
        with col3:
            st.metric("Operating Efficiency", f"{asset_data['operating_efficiency'].mean():.1f}%")
        
        # Time series charts
        st.subheader("Generation & Emissions Trend")
        fig = px.scatter(asset_data, x='generation_mw', y='emissions_co2_tonnes', 
                        color='operating_efficiency', trendline="ols",
                        title=f'Generation vs Emissions - {selected_asset}')
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    create_dashboard()

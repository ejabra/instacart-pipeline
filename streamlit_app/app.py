import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
from config import APP_CONFIG, KAFKA_CONFIG
from utils import (
    execute_query,
    get_kpi_globaux,
    get_top_produits,
    get_ventes_par_departement,
    get_commandes_par_heure,
    get_evolution_temps_reel,
    create_kafka_consumer,
    get_latest_kafka_messages
)

def scroll_to_section(section_id):
    """Scroll vers une section spécifique"""
    js = f"""
    <script>
        var element = document.getElementById('{section_id}');
        if (element) {{
            element.scrollIntoView({{behavior: 'smooth', block: 'start'}});
        }}
    </script>
    """
    st.markdown(js, unsafe_allow_html=True)

# ==================== CONFIGURATION ====================

st.set_page_config(
    page_title=APP_CONFIG['page_title'],
    page_icon=APP_CONFIG['page_icon'],
    layout=APP_CONFIG['layout']
)

# CSS personnalisé 
st.markdown("""
<style>
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1f2e 0%, #2c3e50 100%);
    }
    
     /* Boutons de navigation dans la sidebar */
    [data-testid="stSidebar"] button[kind="secondary"] {
        background: rgba(255, 255, 255, 0.03);
        color: #bdc3c7;
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 10px;
        padding: 12px 16px;
        font-size: 0.95rem;
        text-align: left !important; 
        transition: all 0.3s ease;
        margin-bottom: 8px;
    }
    [data-testid="stSidebar"] button[kind="secondary"]:hover {
        background: rgba(255, 215, 0, 0.1);
        border-color: rgba(255, 215, 0, 0.3);
        color: #ecf0f1;
        transform: translateX(4px);
    }
    
    /* Navigation radio buttons */
    [data-testid="stSidebar"] .stRadio > label {
        color: white !important;
        font-weight: 500;
    }
    [data-testid="stSidebar"] .stRadio > div {
        gap: 8px;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label {
        background: rgba(255, 255, 255, 0.03);
        padding: 14px 18px;
        border-radius: 12px;
        transition: all 0.3s ease;
        cursor: pointer;
        border: 1px solid rgba(255, 255, 255, 0.05);
        font-size: 0.95rem;
        color: #bdc3c7 !important;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:hover {
        background: rgba(255, 215, 0, 0.1);
        border-color: rgba(255, 215, 0, 0.3);
        transform: translateX(4px);
        color: #ecf0f1 !important;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label[data-checked="true"] {
        background: linear-gradient(135deg, rgba(255, 215, 0, 0.2) 0%, rgba(255, 165, 0, 0.15) 100%);
        border-left: 4px solid #FFD700;
        color: #FFD700 !important;
        font-weight: 600;
        box-shadow: 0 4px 12px rgba(255, 215, 0, 0.15);
    }
    
    /* Selectbox et inputs */
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stDateInput label {
        color: #bdc3c7 !important;
        font-size: 0.85rem;
        font-weight: 500;
    }
    [data-testid="stSidebar"] .stSelectbox > div > div,
    [data-testid="stSidebar"] .stDateInput > div > div {
        background-color: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
    }
    
    /* Checkbox et slider */
    [data-testid="stSidebar"] .stCheckbox label {
        color: #ecf0f1 !important;
        font-weight: 500;
    }
    [data-testid="stSidebar"] .stSlider label {
        color: #bdc3c7 !important;
        font-size: 0.85rem;
    }
    
    /* Boutons */
    [data-testid="stSidebar"] button[kind="primary"] {
        background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%);
        color: #1a1f2e;
        font-weight: 600;
        border: none;
        border-radius: 10px;
        padding: 12px;
        box-shadow: 0 4px 12px rgba(255, 215, 0, 0.3);
        transition: all 0.3s ease;
    }
    [data-testid="stSidebar"] button[kind="primary"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(255, 215, 0, 0.4);
    }
    
    /* Cartes KPI */
    .kpi-card {
        background: linear-gradient(135deg, #2C3E50 0%, #34495E 100%);
        padding: 25px 20px;
        border-radius: 12px;
        text-align: center;
        color: white;
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
        border: 1px solid rgba(255, 255, 255, 0.05);
        transition: transform 0.3s ease;
    }
    .kpi-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.2);
    }
    .kpi-value {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FFD700;
        margin: 12px 0;
        text-shadow: 0 2px 8px rgba(255, 215, 0, 0.3);
    }
    .kpi-label {
        font-size: 0.85rem;
        color: #bdc3c7;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 500;
    }
    
    /* Timestamp */
    .update-time {
        text-align: right;
        color: #7F8C8D;
        font-size: 0.8rem;
        font-style: italic;
        padding: 8px 0;
    }
    
    /* Animation LIVE */
    @keyframes blink-live {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.3; }
    }
    .update-time span {
        animation: blink-live 2s infinite;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ==================== HEADER ====================

# Sidebar avec logo et navigation
with st.sidebar:
    # Logo et titre
    col_logo_left, col_logo_center, col_logo_right = st.columns([1, 2, 1])
    with col_logo_center:
        st.image("logo.png", width=80)
    
    st.markdown("""
    <div style='text-align: center; padding: 0px 0;'>
        <h2 style='color: #FFD700; margin-top: 0px; font-weight: 600; letter-spacing: 1px;'>Data Makers</h2>
        <p style='color: #95a5a6; font-size: 0.85rem; margin-top: 0px;'>Real-Time Analytics</p>
    </div>
                <div> </div>
    """, unsafe_allow_html=True)
      
    # Boutons de navigation
    if st.button("Dashboard", use_container_width=True, type="secondary", key="nav_dash"):
        st.session_state.page = "dashboard"
        st.rerun()
    
    # LIENS HTML POUR SCROLL (au lieu de boutons Streamlit)
    st.markdown("""
    <a href="#top-10-produits" style="text-decoration: none;">
        <div style="width: 100%; background: rgba(255, 255, 255, 0.03); color: #bdc3c7; 
                    border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 10px; 
                    padding: 12px 16px; font-size: 0.95rem; text-align: center; 
                    cursor: pointer; margin-bottom: 25px; transition: all 0.3s ease;">
            Produits
        </div>
    </a>
    <a href="#flux-en-temps-reel" style="text-decoration: none;">
        <div style="width: 100%; background: rgba(255, 255, 255, 0.03); color: #bdc3c7; 
                    border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 10px; 
                    padding: 12px 16px; font-size: 0.95rem; text-align: center; 
                    cursor: pointer; margin-bottom: 25px; transition: all 0.3s ease;">
            Temps Réel
        </div>
    </a>
    <style>
    a > div:hover {
        background: rgba(255, 215, 0, 0.1) !important;
        border-color: rgba(255, 215, 0, 0.3) !important;
        color: #ecf0f1 !important;
        transform: translateX(4px);
    }
    </style>
    """, unsafe_allow_html=True)
    
    if st.button("Qualité Données", use_container_width=True, type="secondary", key="nav_quality"):
        st.session_state.page = "quality"
        st.rerun()

# Initialiser la page
if 'page' not in st.session_state:
    st.session_state.page = "dashboard"

page = st.session_state.get('page', 'dashboard')


if page == "dashboard":
    st.title("📊 InstaCart - Dashboard Temps Réel")
    st.markdown("**Analyse des Commandes en Direct** | Données actualisées automatiquement")
    st.divider()
    
    # Bouton Filtrer au-dessus de la mise à jour
    col_filter_spacer, col_filter_toggle = st.columns([9, 1])
    with col_filter_toggle:
        if st.button("🔍 Filtrer", type="secondary", use_container_width=True, key="toggle_filters_btn"):
            st.session_state.show_filters = not st.session_state.show_filters
            st.rerun()

    # Initialiser show_filters
    if 'show_filters' not in st.session_state:
        st.session_state.show_filters = False
    
    if st.session_state.show_filters:
        col_f1, col_f2 = st.columns(2)
        
        with col_f1:
            # Filtre Département
            st.markdown("<span style='font-size:0.9rem; font-weight:600;'>📦 Filtre Département</span>", unsafe_allow_html=True)
            departements_df = execute_query("""
                SELECT department, COUNT(*) AS nb_commandes
                FROM instacart_flat_data
                GROUP BY department
                ORDER BY nb_commandes DESC
            """)
            
            if not departements_df.empty:
                departements_liste = departements_df['department'].tolist()
                filtre_dept = st.multiselect(
                    "Sélectionnez un ou plusieurs départements",
                    options=departements_liste,
                    default=[],
                    key="filter_dept",
                    label_visibility="collapsed"
                )
            else:
                filtre_dept = []
        
        with col_f2:
            # Filtre Période
            st.markdown("<span style='font-size:0.9rem; font-weight:600;'>📅 Filtre Période</span>", unsafe_allow_html=True)
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                label_d1, input_d1 = st.columns([1, 4])
                with label_d1:
                    st.markdown("<span style='font-size:0.85rem;'>Du</span>", unsafe_allow_html=True)
                with input_d1:
                    date_min = st.date_input("Du", value=None, key="date_from", label_visibility="collapsed")
            with col_d2:
                label_d2, input_d2 = st.columns([1, 4])
                with label_d2:
                    st.markdown("<span style='font-size:0.85rem;'>Au</span>", unsafe_allow_html=True)
                with input_d2:
                    date_max = st.date_input("Au", value=None, key="date_to", label_visibility="collapsed")
        
        # Afficher barre filtres actifs + bouton effacer
        has_filters = (filtre_dept and len(filtre_dept) > 0) or date_min or date_max
        
        if has_filters:
            filters_text = []
            if filtre_dept and len(filtre_dept) > 0:
                dept_str = ", ".join(filtre_dept)
                filters_text.append(f"📦 {dept_str}")
            if date_min:
                filters_text.append(f"📅 Du {date_min}")
            if date_max:
                filters_text.append(f"📅 Au {date_max}")
            
            col_filter_info, col_filter_btn = st.columns([4, 1])
            with col_filter_info:
                st.info(f"🔍 **Filtres actifs** : {' | '.join(filters_text)}")
            with col_filter_btn:
                if st.button("🗑️ Effacer", type="secondary", use_container_width=True, key="clear_filters_btn"):
                    for key in ['filter_dept', 'date_from', 'date_to']:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
        else:
            filtre_dept = []
            date_min = None
            date_max = None
    else:
        filtre_dept = []
        date_min = None
        date_max = None
    

elif page == "quality":
    st.title("📊 Qualité des Données")
    st.markdown("**Monitoring et Contrôle Qualité du Pipeline**")
    st.divider()

# ==================== AUTO-REFRESH ====================

# Placeholder pour l'heure de mise à jour
placeholder_time = st.empty()

# Initialiser le consumer Kafka (une seule fois)
if 'kafka_consumer' not in st.session_state:
    st.session_state.kafka_consumer = create_kafka_consumer()

# Container principal avec auto-refresh
main_container = st.container()

# ==================== FONCTION PRINCIPALE ====================

def render_dashboard(filtre_dept=[], date_min=None, date_max=None):
    """Fonction qui rend tout le dashboard avec filtres appliqués"""
    
    current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    # Indicateur LIVE toujours actif
    auto_refresh_status = " 🔴 <span style='color: #FF4444;'>LIVE</span>"
    
    placeholder_time.markdown(
        f'<div class="update-time">⏱️ Dernière mise à jour : {current_time}{auto_refresh_status}</div>',
        unsafe_allow_html=True
    )
    
    # ==================== SECTION 1 : KPIs GLOBAUX ====================
    
    st.markdown('<h2 id="indicateurs-cles">📈 Indicateurs Clés</h2>', unsafe_allow_html=True)
    
    kpis = get_kpi_globaux(filtre_dept, date_min, date_max)
    
    if not kpis.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Nombre de Commandes</div>
                <div class="kpi-value">{kpis['nb_commandes'].iloc[0]:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Articles Vendus</div>
                <div class="kpi-value">{kpis['nb_produits_vendus'].iloc[0]:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Nombre de Clients</div>
                <div class="kpi-value">{kpis['nb_clients'].iloc[0]:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Panier Moyen</div>
                <div class="kpi-value">{kpis['panier_moyen'].iloc[0]}</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.divider()
    
    # ==================== SECTION 2 : GRAPHIQUES ====================
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown('<h3 id="top-10-produits">🏆 Top 10 Produits</h3>', unsafe_allow_html=True)
        top_produits = get_top_produits(10, filtre_dept, date_min, date_max)
        if not top_produits.empty:
            fig = px.bar(
                top_produits,
                x='nb_ventes',
                y='product_name',
                orientation='h',
                color='nb_ventes',
                color_continuous_scale=['#FFD700', '#FFA500', '#FF6347'],
                labels={'nb_ventes': 'Nombre de Ventes', 'product_name': 'Produit'}
            )
            fig.update_layout(
                showlegend=False,
                height=400,
                margin=dict(l=0, r=0, t=30, b=0),
                yaxis={'categoryorder': 'total ascending'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col_right:
        st.markdown("### 📦 Répartition par Département")
        ventes_dept = get_ventes_par_departement(filtre_dept, date_min, date_max)
        if not ventes_dept.empty:
            fig = px.pie(
                ventes_dept.head(10),
                values='nb_ventes',
                names='department',
                color_discrete_sequence=px.colors.sequential.YlOrRd
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # ==================== SECTION 3 : ACTIVITÉ TEMPS RÉEL ====================
    
    st.markdown('<h2 id="flux-en-temps-reel">⚡ Flux en Temps Réel</h2>', unsafe_allow_html=True)
    
    col_stream, col_kafka = st.columns([2, 1])
    
    with col_stream:
        st.markdown("### 📊 Distribution par Heure")
        commandes_heure = get_commandes_par_heure(filtre_dept, date_min, date_max)
        if not commandes_heure.empty:
            fig = px.area(
                commandes_heure,
                x='hour',
                y='nb_commandes',
                color_discrete_sequence=['#FFD700'],
                labels={'hour': 'Heure de la Journée', 'nb_commandes': 'Nombre de Commandes'}
            )
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=30, b=0),
                xaxis=dict(range=[0, 23], dtick=2)
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col_kafka:
        st.markdown("### 🔴 Flux de Produits en Live")
        
        # Initialiser le buffer de messages dans session state
        if 'kafka_messages_buffer' not in st.session_state:
            st.session_state.kafka_messages_buffer = []
        
        # Vérifier la connexion Kafka
        if st.session_state.kafka_consumer:
            try:
                # Récupérer les nouveaux messages
                new_messages = get_latest_kafka_messages(st.session_state.kafka_consumer, max_messages=20)
                
                # Ajouter au buffer et garder seulement les 50 derniers
                if new_messages and len(new_messages) > 0:
                    st.session_state.kafka_messages_buffer.extend(new_messages)
                    st.session_state.kafka_messages_buffer = st.session_state.kafka_messages_buffer[-50:]
                
                # Afficher les messages du buffer
                messages = st.session_state.kafka_messages_buffer
                
                if messages and len(messages) > 0:
                    # Compteur de produits
                    st.metric("✅️ Messages reçus", len(messages), delta=f"+{len(new_messages)} nouveau(x)")
                    
                    # Bouton pour vider le buffer
                    if st.button("🗑️ Vider Buffer", key="clear_kafka", use_container_width=True, type="secondary"):
                        st.session_state.kafka_messages_buffer = []
                        st.rerun()
                    
                    # Afficher les 10 derniers messages
                    st.caption(f"Affichage des 10 derniers messages (sur {len(messages)} total)")
                    for i, msg in enumerate(list(reversed(messages))[:10], 1):
                        with st.expander(f"✅️ Message #{i}", expanded=(i==1)):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown(f"**🛒 Commande:** {msg.get('order_id', 'N/A')}")
                                st.markdown(f"**📦 Produit ID:** {msg.get('product_id', 'N/A')}")
                                st.markdown(f"**#️⃣ Position:** {msg.get('add_to_cart_order', 'N/A')}")
                            with col2:
                                reordered = "✅ Oui" if msg.get('reordered') == 1 else "❌ Non"
                                st.markdown(f"**🔄 Réacheté:** {reordered}")
                                st.markdown(f"**🕐 Heure:** {msg.get('event_time', 'N/A')}")
                            # Debug : afficher le message brut
                            with st.expander("🔍 Message brut (debug)"):
                                st.json(msg)
                else:
                    st.info("⏳ Aucun message Kafka reçu")
                    
            except Exception as e:
                st.error(f"❌ Erreur Kafka : {str(e)}")
                st.caption("Essayez de reconnecter le consumer")
        else:
            st.warning("⚠️ Kafka non connecté")
            if st.button("🔄 Reconnecter", key="retry_kafka_main"):
                st.session_state.kafka_consumer = create_kafka_consumer()
                st.rerun()
    
        st.divider()
    
    # ==================== SECTION 4 : DERNIÈRES COMMANDES ====================
    
    st.markdown("## 📋 Les 100 derniers Produits Vendus")
    
    derniers_flux = get_evolution_temps_reel(100, filtre_dept, date_min, date_max)
    if not derniers_flux.empty:
        st.dataframe(
            derniers_flux,
            use_container_width=True,
            height=550,
            column_config={
                "event_time": st.column_config.DatetimeColumn(
                    "Horodatage",
                    format="DD/MM/YYYY HH:mm:ss"
                ),
                "order_id": "ID Commande",
                "user_id": "Client",
                "product_name": "Produit",
                "department": "Rayon"
            }
        )


# ==================== BOUCLE PRINCIPALE ====================

# Afficher le contenu selon la page
if page == "dashboard":
    with main_container:
        render_dashboard(filtre_dept, date_min, date_max)
    
    # ==================== AUTO-REFRESH POUR STREAMING ====================
    # Auto-refresh automatique toutes les X secondes
    time.sleep(APP_CONFIG['refresh_interval'])
    st.rerun()

elif page == "quality":
    # ==================== PAGE QUALITÉ DES DONNÉES ====================
    
    st.markdown("## 🔍 Vue d'Ensemble")
    
    # Statistiques globales
    stats_quality = execute_query("""
    SELECT 
        COUNT(*) AS total_lignes,
        COUNT(DISTINCT order_id) AS nb_commandes,
        COUNT(DISTINCT user_id) AS nb_clients,
        COUNT(DISTINCT product_id) AS nb_produits_uniques,
        MIN(event_time) AS premiere_commande,
        MAX(event_time) AS derniere_commande
    FROM instacart_flat_data
    """)
    
    if not stats_quality.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📊 Total Lignes", f"{stats_quality['total_lignes'].iloc[0]:,}")
        with col2:
            st.metric("🛒 Commandes", f"{stats_quality['nb_commandes'].iloc[0]:,}")
        with col3:
            st.metric("👥 Clients", f"{stats_quality['nb_clients'].iloc[0]:,}")
        with col4:
            st.metric("📦 Produits Uniques", f"{stats_quality['nb_produits_uniques'].iloc[0]:,}")
        
        st.info(f"📅 Période des données : {stats_quality['premiere_commande'].iloc[0]} → {stats_quality['derniere_commande'].iloc[0]}")
    
    st.divider()
    
    # ==================== COMPLÉTUDE DES DONNÉES ====================
    
    st.markdown("## ✅ Complétude des Données")
    
    completeness_query = """
    SELECT 
        'order_id' AS colonne,
        COUNT(*) AS total,
        COUNT(order_id) AS non_null,
        ROUND(COUNT(order_id) * 100.0 / COUNT(*), 2) AS taux_completude
    FROM instacart_flat_data
    UNION ALL
    SELECT 
        'product_id',
        COUNT(*),
        COUNT(product_id),
        ROUND(COUNT(product_id) * 100.0 / COUNT(*), 2)
    FROM instacart_flat_data
    UNION ALL
    SELECT 
        'user_id',
        COUNT(*),
        COUNT(user_id),
        ROUND(COUNT(user_id) * 100.0 / COUNT(*), 2)
    FROM instacart_flat_data
    UNION ALL
    SELECT 
        'event_time',
        COUNT(*),
        COUNT(event_time),
        ROUND(COUNT(event_time) * 100.0 / COUNT(*), 2)
    FROM instacart_flat_data
    """
    
    completeness_df = execute_query(completeness_query)
    
    if not completeness_df.empty:
        # Graphique de complétude
        fig = px.bar(
            completeness_df,
            x='colonne',
            y='taux_completude',
            color='taux_completude',
            color_continuous_scale=['#FF6347', '#FFD700', '#90EE90'],
            labels={'taux_completude': 'Taux de Complétude (%)', 'colonne': 'Colonne'},
            title="Taux de Complétude par Colonne"
        )
        fig.update_layout(height=350, showlegend=False)
        fig.add_hline(y=95, line_dash="dash", line_color="green", annotation_text="Seuil acceptable (95%)")
        st.plotly_chart(fig, use_container_width=True)
        
        # Tableau détaillé
        st.dataframe(
            completeness_df,
            use_container_width=True,
            column_config={
                "colonne": "Colonne",
                "total": st.column_config.NumberColumn("Total Lignes", format="%d"),
                "non_null": st.column_config.NumberColumn("Valeurs Non-Null", format="%d"),
                "taux_completude": st.column_config.ProgressColumn(
                    "Complétude",
                    format="%.2f%%",
                    min_value=0,
                    max_value=100
                )
            }
        )
    
    st.divider()
    
    # ==================== DÉTECTION D'ANOMALIES ====================
    
    st.markdown("## 🚨 Détection d'Anomalies")
    
    col_anom1, col_anom2 = st.columns(2)
    
    with col_anom1:
        st.markdown("### 📦 Commandes Suspectes")
        
        # Commandes avec trop de produits
        anomalies_panier = execute_query("""
        SELECT 
            order_id,
            user_id,
            COUNT(*) AS nb_produits
        FROM instacart_flat_data
        GROUP BY order_id, user_id
        HAVING COUNT(*) > 50
        ORDER BY nb_produits DESC
        LIMIT 10
        """)
        
        if not anomalies_panier.empty:
            st.warning(f"⚠️ {len(anomalies_panier)} commande(s) avec >50 produits détectées")
            st.dataframe(anomalies_panier, use_container_width=True)
        else:
            st.success("✅ Aucune anomalie de panier détectée")
    
    with col_anom2:
        st.markdown("### 👥 Clients Suspects")
        
        # Clients avec activité anormale
        anomalies_clients = execute_query("""
        SELECT 
            user_id,
            COUNT(DISTINCT order_id) AS nb_commandes,
            COUNT(*) AS nb_produits_total
        FROM instacart_flat_data
        GROUP BY user_id
        HAVING COUNT(DISTINCT order_id) > 100
        ORDER BY nb_commandes DESC
        LIMIT 10
        """)
        
        if not anomalies_clients.empty:
            st.warning(f"⚠️ {len(anomalies_clients)} client(s) avec >100 commandes")
            st.dataframe(anomalies_clients, use_container_width=True)
        else:
            st.success("✅ Aucune anomalie client détectée")
    
    st.divider()
    
    # ==================== DISTRIBUTION TEMPORELLE ====================
    
    st.markdown("## 📅 Distribution Temporelle")
    
    # Commandes par jour
    daily_dist = execute_query("""
    SELECT 
        toDate(event_time) AS date,
        COUNT(DISTINCT order_id) AS nb_commandes
    FROM instacart_flat_data
    GROUP BY toDate(event_time)
    ORDER BY date
    """)
    
    if not daily_dist.empty:
        fig = px.line(
            daily_dist,
            x='date',
            y='nb_commandes',
            title="Évolution Quotidienne des Commandes",
            labels={'date': 'Date', 'nb_commandes': 'Nombre de Commandes'}
        )
        fig.update_traces(line_color='#FFD700', line_width=3)
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # ==================== STATUT DU PIPELINE ====================
    
    st.markdown("## 🔧 Statut du Pipeline")
    
    col_pipe1, col_pipe2, col_pipe3 = st.columns(3)
    
    with col_pipe1:
        # Test Clickhouse
        try:
            test_ch = execute_query("SELECT 1")
            if not test_ch.empty:
                st.success("✅ Clickhouse : OK")
            else:
                st.error("❌ Clickhouse : Erreur")
        except:
            st.error("❌ Clickhouse : Inaccessible")
    
    with col_pipe2:
        # Test Kafka
        if st.session_state.kafka_consumer:
            st.success("✅ Kafka : Connecté")
        else:
            st.error("❌ Kafka : Déconnecté")
    
    with col_pipe3:
        # Fraîcheur des données
        freshness = execute_query("""
        SELECT 
            MAX(event_time) AS derniere_maj,
            now() AS maintenant,
            dateDiff('minute', MAX(event_time), now()) AS minutes_depuis_maj
        FROM instacart_flat_data
        """)
        
        if not freshness.empty:
            minutes = freshness['minutes_depuis_maj'].iloc[0]
            if minutes < 5:
                st.success(f"✅ Données : Fraîches ({minutes} min)")
            elif minutes < 60:
                st.warning(f"⚠️ Données : {minutes} min")
            else:
                st.error(f"❌ Données : Anciennes ({minutes} min)")
        else:
            st.error("❌ Impossible de vérifier")

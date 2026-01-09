import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import pandas as pd
import numpy as np
from datetime import datetime

import sys
sys.path.append('.')
from db import get_connection


plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = 'white'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['axes.labelsize'] = 10

COLORS = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B', 
          '#95C623', '#5C4D7D', '#E84855', '#F9DC5C', '#3185FC']


def get_dataframe(query):
    """Execute query and return DataFrame"""
    conn = get_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df



def kpi_sales_by_category():
    """Graphique en barres - Ventes par catégorie"""
    query = """
        SELECT 
            COALESCE(p.category, 'Non catégorisé') as category,
            SUM(f.sales_amount) as total_sales,
            SUM(f.quantity) as total_quantity,
            COUNT(*) as nb_transactions
        FROM gold.fact_sales f
        JOIN gold.dim_products p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY total_sales DESC
    """
    df = get_dataframe(query)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['category'], df['total_sales'], color=COLORS[:len(df)])
    
    for bar, val in zip(bars, df['total_sales']):
        ax.text(bar.get_width() + 1000, bar.get_y() + bar.get_height()/2, 
                f'{val:,.0f} €', va='center', fontsize=9)
    
    ax.set_xlabel('Chiffre d\'affaires (€)')
    ax.set_title('💰 Ventes totales par catégorie de produit', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()
    return fig



def kpi_sales_by_country():
    """Graphique camembert - Répartition par pays"""
    query = """
        SELECT 
            COALESCE(c.country, 'Inconnu') as country,
            SUM(f.sales_amount) as total_sales
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        GROUP BY c.country
        ORDER BY total_sales DESC
        LIMIT 8
    """
    df = get_dataframe(query)
    
    fig, ax = plt.subplots(figsize=(10, 8))
    wedges, texts, autotexts = ax.pie(
        df['total_sales'], 
        labels=df['country'],
        autopct='%1.1f%%',
        colors=COLORS[:len(df)],
        explode=[0.05] * len(df),
        shadow=True,
        startangle=90
    )
    
    for autotext in autotexts:
        autotext.set_fontsize(9)
        autotext.set_fontweight('bold')
    
    ax.set_title('🌍 Répartition des ventes par pays', fontsize=14, fontweight='bold')
    plt.tight_layout()
    return fig



def kpi_sales_over_time():
    """Graphique courbe - Évolution temporelle des ventes"""
    query = """
        SELECT 
            DATE_TRUNC('month', order_date) as month,
            SUM(sales_amount) as total_sales,
            COUNT(DISTINCT order_number) as nb_orders
        FROM gold.fact_sales
        WHERE order_date IS NOT NULL
        GROUP BY DATE_TRUNC('month', order_date)
        ORDER BY month
    """
    df = get_dataframe(query)
    df['month'] = pd.to_datetime(df['month'])
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    color1 = COLORS[0]
    ax1.plot(df['month'], df['total_sales'], color=color1, linewidth=2, marker='o', markersize=4)
    ax1.fill_between(df['month'], df['total_sales'], alpha=0.3, color=color1)
    ax1.set_xlabel('Période')
    ax1.set_ylabel('Chiffre d\'affaires (€)', color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)
    
    ax2 = ax1.twinx()
    color2 = COLORS[1]
    ax2.bar(df['month'], df['nb_orders'], alpha=0.3, color=color2, width=20)
    ax2.set_ylabel('Nombre de commandes', color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    ax1.set_title('📈 Évolution des ventes dans le temps', fontsize=14, fontweight='bold')
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    return fig


def kpi_top_products():
    """Graphique en barres horizontales - Top produits"""
    query = """
        SELECT 
            p.product_name,
            SUM(f.sales_amount) as total_sales,
            SUM(f.quantity) as total_quantity
        FROM gold.fact_sales f
        JOIN gold.dim_products p ON f.product_key = p.product_key
        GROUP BY p.product_name
        ORDER BY total_sales DESC
        LIMIT 10
    """
    df = get_dataframe(query)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    df['product_short'] = df['product_name'].apply(lambda x: x[:30] + '...' if len(str(x)) > 30 else x)
    
    bars = ax.barh(df['product_short'], df['total_sales'], color=COLORS[0])
    
    for bar, val in zip(bars, df['total_sales']):
        ax.text(bar.get_width() + 500, bar.get_y() + bar.get_height()/2,
                f'{val:,.0f} €', va='center', fontsize=9)
    
    ax.set_xlabel('Chiffre d\'affaires (€)')
    ax.set_title('🏆 Top 10 des produits les plus vendus', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()
    return fig



def kpi_top_customers():
    """Graphique en barres - Top clients"""
    query = """
        SELECT 
            c.first_name || ' ' || c.last_name as customer_name,
            c.country,
            SUM(f.sales_amount) as total_spent,
            COUNT(DISTINCT f.order_number) as nb_orders
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        GROUP BY c.customer_key, c.first_name, c.last_name, c.country
        ORDER BY total_spent DESC
        LIMIT 10
    """
    df = get_dataframe(query)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(df))
    bars = ax.bar(x, df['total_spent'], color=COLORS[2])
    
    ax.set_xticks(x)
    ax.set_xticklabels(df['customer_name'], rotation=45, ha='right')
    ax.set_ylabel('Total dépensé (€)')
    ax.set_title('👥 Top 10 des meilleurs clients', fontsize=14, fontweight='bold')
    
    for i, (bar, country) in enumerate(zip(bars, df['country'])):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 500,
                f'{country}', ha='center', va='bottom', fontsize=8, color='gray')
    
    plt.tight_layout()
    return fig



def kpi_sales_by_gender():
    """Graphique camembert - Ventes par genre"""
    query = """
        SELECT 
            c.gender,
            SUM(f.sales_amount) as total_sales,
            COUNT(DISTINCT c.customer_key) as nb_customers
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        GROUP BY c.gender
        ORDER BY total_sales DESC
    """
    df = get_dataframe(query)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    colors_gender = {'Male': COLORS[0], 'Female': COLORS[1], 'n/a': COLORS[7]}
    pie_colors = [colors_gender.get(g, COLORS[5]) for g in df['gender']]
    
    ax1.pie(df['total_sales'], labels=df['gender'], autopct='%1.1f%%',
            colors=pie_colors, explode=[0.02]*len(df), shadow=True)
    ax1.set_title('Ventes par genre', fontweight='bold')
    
    ax2.pie(df['nb_customers'], labels=df['gender'], autopct='%1.1f%%',
            colors=pie_colors, explode=[0.02]*len(df), shadow=True)
    ax2.set_title('Clients par genre', fontweight='bold')
    
    fig.suptitle('Analyse par genre', fontsize=14, fontweight='bold')
    plt.tight_layout()
    return fig



def kpi_sales_by_product_line():
    """Graphique en barres groupées - Ventes par ligne de produit"""
    query = """
        SELECT 
            p.product_line,
            SUM(f.sales_amount) as total_sales,
            SUM(f.quantity) as total_quantity,
            AVG(f.price) as avg_price
        FROM gold.fact_sales f
        JOIN gold.dim_products p ON f.product_key = p.product_key
        GROUP BY p.product_line
        ORDER BY total_sales DESC
    """
    df = get_dataframe(query)
    
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    axes[0].bar(df['product_line'], df['total_sales'], color=COLORS[0])
    axes[0].set_title('Chiffre d\'affaires', fontweight='bold')
    axes[0].set_ylabel('€')
    axes[0].tick_params(axis='x', rotation=45)
    
    
    axes[1].bar(df['product_line'], df['total_quantity'], color=COLORS[1])
    axes[1].set_title('Quantités vendues', fontweight='bold')
    axes[1].set_ylabel('Unités')
    axes[1].tick_params(axis='x', rotation=45)
    
    axes[2].bar(df['product_line'], df['avg_price'], color=COLORS[2])
    axes[2].set_title('Prix moyen', fontweight='bold')
    axes[2].set_ylabel('€')
    axes[2].tick_params(axis='x', rotation=45)
    
    fig.suptitle('📦 Analyse par ligne de produit', fontsize=14, fontweight='bold')
    plt.tight_layout()
    return fig



def kpi_sales_by_marital_status():
    """Histogramme - Ventes par statut marital"""
    query = """
        SELECT 
            c.marital_status,
            SUM(f.sales_amount) as total_sales,
            AVG(f.sales_amount) as avg_order_value,
            COUNT(*) as nb_transactions
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        GROUP BY c.marital_status
    """
    df = get_dataframe(query)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    bars1 = ax1.bar(df['marital_status'], df['total_sales'], color=COLORS[:len(df)])
    ax1.set_title('Ventes totales', fontweight='bold')
    ax1.set_ylabel('Chiffre d\'affaires (€)')
    
    for bar, val in zip(bars1, df['total_sales']):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f'{val/1e6:.1f}M€', ha='center', va='bottom', fontsize=9)
    
    bars2 = ax2.bar(df['marital_status'], df['avg_order_value'], color=COLORS[:len(df)])
    ax2.set_title('Panier moyen', fontweight='bold')
    ax2.set_ylabel('Valeur moyenne (€)')
    
    for bar, val in zip(bars2, df['avg_order_value']):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                f'{val:.0f}€', ha='center', va='bottom', fontsize=9)
    
    fig.suptitle('💍 Analyse par statut marital', fontsize=14, fontweight='bold')
    plt.tight_layout()
    return fig



def kpi_dashboard_summary():
    """Dashboard récapitulatif avec KPI principaux"""
    
    query_global = """
        SELECT 
            SUM(sales_amount) as total_revenue,
            COUNT(DISTINCT order_number) as total_orders,
            COUNT(DISTINCT customer_key) as total_customers,
            AVG(sales_amount) as avg_order_value,
            SUM(quantity) as total_units
        FROM gold.fact_sales
    """
    global_metrics = get_dataframe(query_global).iloc[0]
    
    query_top_cat = """
        SELECT p.category, SUM(f.sales_amount) as sales
        FROM gold.fact_sales f
        JOIN gold.dim_products p ON f.product_key = p.product_key
        GROUP BY p.category ORDER BY sales DESC LIMIT 1
    """
    top_cat = get_dataframe(query_top_cat)
    
    query_top_country = """
        SELECT c.country, SUM(f.sales_amount) as sales
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        GROUP BY c.country ORDER BY sales DESC LIMIT 1
    """
    top_country = get_dataframe(query_top_country)
    
    fig = plt.figure(figsize=(16, 10))
    gs = GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)
    
    # Ligne 1: KPI Cards
    ax_revenue = fig.add_subplot(gs[0, 0])
    ax_orders = fig.add_subplot(gs[0, 1])
    ax_customers = fig.add_subplot(gs[0, 2])
    
    # Card Style
    for ax, value, label, color in [
        (ax_revenue, f"{global_metrics['total_revenue']/1e6:.2f}M €", "Chiffre d'affaires", COLORS[0]),
        (ax_orders, f"{global_metrics['total_orders']:,.0f}", "Commandes", COLORS[1]),
        (ax_customers, f"{global_metrics['total_customers']:,.0f}", "Clients", COLORS[2])
    ]:
        ax.text(0.5, 0.6, value, ha='center', va='center', fontsize=28, fontweight='bold', color=color)
        ax.text(0.5, 0.25, label, ha='center', va='center', fontsize=12, color='gray')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        ax.add_patch(plt.Rectangle((0.05, 0.05), 0.9, 0.9, fill=False, edgecolor=color, linewidth=3, transform=ax.transAxes))
    
    # Ligne 2: Ventes par catégorie et pays
    ax_cat = fig.add_subplot(gs[1, :2])
    query_cat = """
        SELECT COALESCE(p.category, 'N/A') as category, SUM(f.sales_amount) as sales
        FROM gold.fact_sales f
        JOIN gold.dim_products p ON f.product_key = p.product_key
        GROUP BY p.category ORDER BY sales DESC LIMIT 5
    """
    df_cat = get_dataframe(query_cat)
    ax_cat.barh(df_cat['category'], df_cat['sales'], color=COLORS[0])
    ax_cat.set_title('Top 5 Catégories', fontweight='bold')
    ax_cat.invert_yaxis()
    
    ax_country = fig.add_subplot(gs[1, 2])
    query_country = """
        SELECT COALESCE(c.country, 'N/A') as country, SUM(f.sales_amount) as sales
        FROM gold.fact_sales f
        JOIN gold.dim_customers c ON f.customer_key = c.customer_key
        GROUP BY c.country ORDER BY sales DESC LIMIT 5
    """
    df_country = get_dataframe(query_country)
    ax_country.pie(df_country['sales'], labels=df_country['country'], autopct='%1.1f%%', colors=COLORS[:5])
    ax_country.set_title('Répartition par pays', fontweight='bold')
    
    ax_time = fig.add_subplot(gs[2, :])
    query_time = """
        SELECT DATE_TRUNC('month', order_date) as month, SUM(sales_amount) as sales
        FROM gold.fact_sales WHERE order_date IS NOT NULL
        GROUP BY DATE_TRUNC('month', order_date) ORDER BY month
    """
    df_time = get_dataframe(query_time)
    df_time['month'] = pd.to_datetime(df_time['month'])
    ax_time.fill_between(df_time['month'], df_time['sales'], alpha=0.3, color=COLORS[0])
    ax_time.plot(df_time['month'], df_time['sales'], color=COLORS[0], linewidth=2)
    ax_time.set_title('Évolution des ventes', fontweight='bold')
    ax_time.set_xlabel('Période')
    ax_time.set_ylabel('Ventes (€)')
    
    fig.suptitle('📊 TABLEAU DE BORD - DATA WAREHOUSE', fontsize=18, fontweight='bold', y=0.98)
    
    return fig



def generate_all_dashboards(save_path='dashboards'):
    import os
    
    # Créer le dossier si nécessaire
    os.makedirs(save_path, exist_ok=True)
    
    print("\n" + "="*60)
    print("   GÉNÉRATION DES TABLEAUX DE BORD")
    print("="*60)
    
    dashboards = [
        ('kpi_dashboard_summary', kpi_dashboard_summary, 'Dashboard récapitulatif'),
        ('kpi_sales_by_category', kpi_sales_by_category, 'Ventes par catégorie'),
        ('kpi_sales_by_country', kpi_sales_by_country, 'Ventes par pays'),
        ('kpi_sales_over_time', kpi_sales_over_time, 'Évolution temporelle'),
        ('kpi_top_products', kpi_top_products, 'Top 10 produits'),
        ('kpi_top_customers', kpi_top_customers, 'Top 10 clients'),
        ('kpi_sales_by_gender', kpi_sales_by_gender, 'Analyse par genre'),
        ('kpi_sales_by_product_line', kpi_sales_by_product_line, 'Analyse par ligne produit'),
        ('kpi_sales_by_marital_status', kpi_sales_by_marital_status, 'Analyse statut marital'),
    ]
    
    for filename, func, description in dashboards:
        try:
            print(f"\n📊 Génération: {description}...")
            fig = func()
            filepath = os.path.join(save_path, f'{filename}.png')
            fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            print(f"   ✓ Sauvegardé: {filepath}")
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
    
    print("\n" + "="*60)
    print("   ✓ TOUS LES DASHBOARDS ONT ÉTÉ GÉNÉRÉS!")
    print(f"   📁 Dossier: {os.path.abspath(save_path)}")
    print("="*60 + "\n")


def show_all_dashboards():
    print("\n📊 Affichage des tableaux de bord...")
    
    dashboards = [
        kpi_dashboard_summary,
        kpi_sales_by_category,
        kpi_sales_by_country,
        kpi_sales_over_time,
        kpi_top_products,
        kpi_top_customers,
        kpi_sales_by_gender,
        kpi_sales_by_product_line,
        kpi_sales_by_marital_status,
    ]
    
    for func in dashboards:
        try:
            func()
        except Exception as e:
            print(f"Erreur {func.__name__}: {e}")
    
    plt.show()


if __name__ == "__main__":
    generate_all_dashboards()
    

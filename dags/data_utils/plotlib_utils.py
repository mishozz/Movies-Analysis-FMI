import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def save_plots_to_pdf(figures):
     with PdfPages('imbd_analysis_visualized.pdf') as pdf:
        create_initial_page(pdf)
        for fig in figures:
            pdf.savefig(fig)
            plt.close(fig)

def create_barplot(x, y, x_label, y_label, barplot_title):
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.bar(x, y)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(barplot_title)
    plt.xticks(rotation=70, ha='left')
    plt.tight_layout()

    return fig

def create_piechart(labels, values, title, legend_title_for_count):
    def autopct_func(pct):
        return ('%1.1f%%' % pct) if pct > 3 else ''

    fig, ax = plt.subplots(figsize=(14, 8))

    wedges, _, _ = ax.pie(
        values,
        autopct=autopct_func,
        startangle=140,
        textprops={'fontsize': 10}
    )

    total = sum(values)
    labels_with_counts = [f"{label} ({value})" for label, value in zip(labels, values)]

    ax.legend(
        wedges,
        labels_with_counts,
        title=f"{legend_title_for_count} (Total: {total})",
        loc="center left",
        bbox_to_anchor=(1, 0, 0.5, 1),
        fontsize=10
    )
    ax.set_title(title, fontsize=14)
    plt.tight_layout()

    return fig

def create_initial_page(pdf):
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('off')
    title = "IMDb Dataset Analysis Report"
    paragraph = ("This document is an automatically generated PDF report from the IMDb Data Analysis Pipeline. \n "
                "It contains various barplots and piecharts that provide a comprehensive analysis of the IMDb dataset. \n"
                "The visualizations included in this report aim to offer insights into trends, genres, titles, and actors within the dataset. \n"
                "We hope this report helps you understand the IMDb dataset.")

    ax.text(0.5, 0.8, title, transform=ax.transAxes, fontsize=28, ha='center', color='blue')
    ax.text(0.05, 0.6, paragraph, fontsize=14, ha='left', va='top', wrap=True, transform=ax.transAxes)

    pdf.savefig(fig)
    plt.close(fig)

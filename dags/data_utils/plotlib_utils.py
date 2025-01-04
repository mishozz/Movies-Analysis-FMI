import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def save_plots_to_pdf(figures):
     with PdfPages('report.pdf') as pdf:
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

def create_piechart(labels, values, title):
    def autopct_func(pct):
        return ('%1.1f%%' % pct) if pct > 3 else ''

    fig, ax = plt.subplots(figsize=(14, 8))

    wedges, _, _ = ax.pie(
        values,
        autopct=autopct_func,
        startangle=140,
        textprops={'fontsize': 10}
    )  
    ax.legend(
        wedges,
        labels,
        title="Genres",
        loc="center left",
        bbox_to_anchor=(1, 0, 0.5, 1),
        fontsize=10
    )
    ax.set_title(title, fontsize=14)
    plt.tight_layout()

    return fig

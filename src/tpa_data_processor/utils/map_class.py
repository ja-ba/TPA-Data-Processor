import folium
import leafmap.foliumap as fol_leafmap
from matplotlib import cm
from matplotlib import colors


class Map_Special(fol_leafmap.Map):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # fol_leafmap.Map.__init__(self, *args, **kwargs)

        self.color_list = []
        self.color_min_value = 0
        self.color_max_value = 0

    def add_circle_markers_from_xy_plus(
        self,
        data,
        color_col,
        x="longitude",
        y="latitude",
        tooltip=None,
        popup=None,
        radius=6,
        **kwargs,
    ):
        """Adds a marker cluster to the map. For a list of options, see https://ipyleaflet.readthedocs.io/en/latest/api_reference/circle_marker.html
        Args:
            data (str | pd.DataFrame): A csv or Pandas DataFrame containing x, y, z values.
            x (str, optional): The column name for the x values. Defaults to "longitude".
            y (str, optional): The column name for the y values. Defaults to "latitude".
            radius (int, optional): The radius of the circle. Defaults to 10.
            popup (list, optional): A list of column names to be used as the popup. Defaults to None.
        """

        # Define color palette function##############################################################################
        data["color_col"] = data[color_col]
        perc = list(data["color_col"].dropna().quantile([0.05, 0.95]))

        # data["color_col"] = data["color_col"].apply(lambda x: np.log(x))
        # min_value = data.loc[data["color_col"]>perc[0],"color_col"].min()
        # max_value = data.loc[data["color_col"]<perc[1],"color_col"].max()
        min_value = perc[0]
        max_value = perc[1]

        def value_to_color(value, min, max, colormap):
            cmap = cm.get_cmap(colormap)
            norm = colors.Normalize(vmin=min, vmax=max)

            return colors.to_hex(cmap(norm(value)))

        data["colors"] = data["color_col"].apply(
            lambda x: value_to_color(x, min_value, max_value, "RdYlGn_r")
        )  # "YlOrRd"
        data.sort_values(by="color_col", inplace=True)

        col_names = data.columns.values.tolist()

        for p in popup:
            if data[p].dtype == "float64":
                data[p] = data[p].round(decimals=3)

        if popup is None:
            popup = col_names

        if not isinstance(popup, list):
            popup = [popup]

        if tooltip is not None:
            if not isinstance(tooltip, list):
                tooltip = [tooltip]

        if x not in col_names:
            raise ValueError(f"x must be one of the following: {', '.join(col_names)}")

        if y not in col_names:
            raise ValueError(f"y must be one of the following: {', '.join(col_names)}")

        for idx, row in data.iterrows():
            html = ""
            for p in popup:
                html = html + "<b>" + p + "</b>" + ": " + str(row[p]) + "<br>"
            popup_html = folium.Popup(html, min_width=100, max_width=100)

            if tooltip is not None:
                html = ""
                for p in tooltip:
                    html = html + "<b>" + p + "</b>" + ": " + str(row[p]) + "<br>"

                tooltip_str = folium.Tooltip(html)
            else:
                tooltip_str = None

            folium.CircleMarker(
                location=[row[y], row[x]],
                stroke=False,
                radius=radius,
                popup=popup_html,
                tooltip=tooltip_str,
                fill=True,
                color=row["colors"],
                opacity=0.7,
                fill_color=row["colors"],
                fill_opacity=0.7,
                **kwargs,
            ).add_to(self)

            self.color_list = list(data["colors"])
            self.color_min_value = min_value
            self.color_max_value = max_value

            # del data
            # gc.collect()


# Create a class which builds a Map List of Map_Special objects to store the different needed maps
class Map_List:
    def __init__(self, df, wm):
        self.df = df
        self.wm = wm

        self.map_dict = {}

    def build_maps(self, color_col="e10"):

        # colors = ["#FFD384", "#FF577F"]

        for date in self.wm["week_total"]:
            temp_df = self.df[self.df.week_total == date]
            temp_df.sort_values(color_col, inplace=True)

            m = Map_Special(center=[52, 10], zoom=6)
            # m.add_colormap(cmap="RdYlGn_r", vmin=perc[0], vmax=perc[1])

            m.add_circle_markers_from_xy_plus(
                data=temp_df,
                x="longitude",
                y="latitude",
                radius=5,
                color_col=color_col,
                popup=["Name", "e10", "e5", "diesel"],
            )
            m.add_colorbar(
                colors=m.color_list, vmin=m.color_min_value, vmax=m.color_max_value
            )

            self.map_dict[date] = m

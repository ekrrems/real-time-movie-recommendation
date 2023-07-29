import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QComboBox, QPushButton
import pandas as pd
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic_name = "movie_recommend_topic"


data = pd.read_csv("recom_df.csv")

class MovieSelectorWidget(QWidget):
    def __init__(self, movie_names):
        super().__init__()
        self.movie_names = movie_names
        self.selected_movie = None  

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Movie Selector")

        # Create a QComboBox to display the movie names
        self.combo_box = QComboBox(self)
        self.combo_box.addItem("Select a movie")
        for movie_name in self.movie_names:
            self.combo_box.addItem(movie_name)

        # Connect a signal for when an item is selected in the QComboBox
        self.combo_box.currentIndexChanged.connect(self.on_item_selected)

        # Create a QPushButton to confirm selection
        self.button_select = QPushButton("Select")
        self.button_select.clicked.connect(self.on_select_clicked)

        # Set up the layout
        layout = QVBoxLayout()
        layout.addWidget(self.combo_box)
        layout.addWidget(self.button_select)
        self.setLayout(layout)


    def on_item_selected(self, index):
        # Get the selected movie name from the QComboBox
        self.selected_movie = self.combo_box.currentText()


    def on_select_clicked(self):
        if self.selected_movie and self.selected_movie != "Select a movie":
            # Print the selected movie name when the "Select" button is clicked
            print(f"Selected Movie: {self.selected_movie}")
            self.kafka_producer(self.selected_movie)
        else:
            print("Please select a movie first!")


    def kafka_producer(self, data):
        try:
            # Send data to the Kafka topic
            producer.send(topic_name, str(data).encode('utf-8'))

        except KeyboardInterrupt:
            print("KeyboardInterrupt: Stopping the producer gracefully.")
            

if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Create a list of movie names (replace with your movie names)
    movie_names = data.title.to_list()

    window = MovieSelectorWidget(movie_names)
    window.show()

    sys.exit(app.exec_())

    # Close the producer
    producer.close()

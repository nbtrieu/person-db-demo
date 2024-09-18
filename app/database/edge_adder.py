from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import threading
from typing import Union, List, Dict, Optional
import pandas as pd
from gremlin_python.process.graph_traversal import GraphTraversalSource, __  # noqa
from gremlin_python.process.traversal import (  # noqa
    Barrier,
    Bindings,
    Cardinality,
    CardinalityValue,
    Column,
    Direction,
    Operator,
    Order,
    P,
    Pop,
    Scope,
    T,
    TextP,
    WithOptions,
)

class EdgeAdder:
    def __init__(self, g: GraphTraversalSource, source_node: str, target_node: str, source_id_col: str, target_id_col: str, edge_label: str, batch_size: int = 100):
        self.g = g
        self.source_node = source_node
        self.target_node = target_node
        self.source_id_col = source_id_col
        self.target_id_col = target_id_col
        self.edge_label = edge_label
        self.batch_size = batch_size

        # Create ID dictionaries
        self.source_id_dict = self.create_id_dict(g, source_node, "uuid")
        self.target_id_dict = self.create_id_dict(g, target_node, "uuid")

        # Lock for progress bar
        self.pbar_lock = threading.Lock()

    def create_id_dict(self, g, node_type, primary_property_label):
        node_list = (
            g.V()
            .has_label(node_type)
            .project("id", primary_property_label)
            .by(__.id_())
            .by(__.values(primary_property_label))
            .toList()
        )
        id_dict = {
            node[primary_property_label]: node["id"] for node in node_list
        }
        return id_dict

    def process_batch(self, batch, pbar, source_id_col, target_id_col):
        for _, row in batch.iterrows():
            source_graph_id = self.source_id_dict.get(row[source_id_col])
            target_values = row[target_id_col]

            # Handle list or single string target values
            if isinstance(target_values, str):
                target_values = [target_values]
            
            for target_value in target_values:
                target_graph_id = self.target_id_dict.get(target_value)
                if source_graph_id is not None and target_graph_id is not None:
                    edge = self.g.V(source_graph_id).addE(self.edge_label).to(__.V(target_graph_id))

                    # Apply properties for this specific row
                    properties = row["edge_properties"]
                    if properties is not None and isinstance(properties, dict):
                        for key, value in properties.items():
                            if pd.notna(value):
                                edge = edge.property(key, value)

                    # Force edge creation and log it
                    edge_created = edge.next()
                    # print(f"Edge created: {edge_created}")

            # Safely update the progress bar
            with self.pbar_lock:
                pbar.update(1)

    def add_edges(self, df: pd.DataFrame):
        total_rows = len(df)
        with ThreadPoolExecutor(max_workers=4) as executor:
            with tqdm(total=total_rows, desc=f"Processing {self.source_node}-{self.target_node} edges") as pbar:
                futures = []
                for i in range(0, total_rows, self.batch_size):
                    batch = df.iloc[i:i + self.batch_size]
                    futures.append(executor.submit(self.process_batch, batch, pbar, self.source_id_col, self.target_id_col))
                
                # Wait for all threads to complete
                for future in futures:
                    future.result()

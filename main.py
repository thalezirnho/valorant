# Setup
from layer_input import valorant_layer_input
from layer_tabular import valorant_layer_tabular

# Call input
session_matchs = valorant_layer_input()

# Call tabular (only if we have new matchids)
if len(session_matchs) >= 1:
    # Run Tabular
    valorant_layer_tabular(matches=session_matchs)
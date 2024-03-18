import json
import uuid
import random

# Parameters for generation


# Number of dictionaries you want in your list
num_dicts = 2

# Possible types for the 'type' field
types = ['type1', 'type2', 'type3']

# Generating the list of dictionaries
random_dicts = [
    {
        'id': str(uuid.uuid4()),        # Generates a random UUID
        'value': random.randint(1, 100), # Random integer between 1 and 100
        'type': random.choice(types)    # Randomly selects a type
    }
    for _ in range(num_dicts)
]

# Displaying the final output
# print(random_dicts)

print(json.dumps(random_dicts, indent=4, sort_keys=True))
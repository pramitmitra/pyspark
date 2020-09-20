def is_less_than_8_characters(item):
    return len(item) < 8

x = ['Python', 'programming', 'is', 'awesome!']
results = []

for item in x:
    if is_less_than_8_characters(item):
        results.append(item)

print(results)

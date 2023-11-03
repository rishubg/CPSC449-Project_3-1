import redis

r = redis.Redis(db=0)

# Retrieve the top 5 players in descending order (highest scores first).
top_players = r.zrevrange("players", 0, 4, withscores=True)

# Positions 1 - 3
top3 = {0: "1st", 1: "2nd", 2: "3rd"}

# Print the top 5 players in the desired format.
for i, (player, score) in enumerate(top_players):
    position = top3.get(i, f"{i+1}th")
    print(f"{position}: {player.decode('utf-8')} -- {int(score)}")
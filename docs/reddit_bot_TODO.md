# Linking

Would be nice to link to the episodes, but the links end up being way too long.

Check with reddit mods if linking is OK.

# Reduce comment size

* Italics rather than bolding?
* Simplify timestamps
	* Simple -- to nearest second
	* More aggressive just give the minute associated with the middle, no start / end times.
* Simplify speakers
	* Unknown => ?
	* Alex Jones => Alex
* Better snippet shortening! See below.
* Short links / redirects to wiki would help a lot
* In wiki redirects for the episodes would be good (e.g. /523 => /523:\_January\_18-19,\_2021 )

# Snippet shortening Improvements.

There's a bug where certian diffs produce split highlights:

|577|N**o****t the way**|00:26:58.432 - 00:26:58.943|Unknown|

Here the diff is between NOT and OUT -- dropping the U means we get N _ **O** _ **T ...**

Would be better to take a slightly more structured approach to highlighting.

The way we handle whitespace and highlights is a hack, can still lead to some very long snippets:

... **Move bitch**. Get out the way Get out the way you see them headlights. I'm going 100 down the highway. You're in the fast lane. You see me knock them curtains down. You see what happens with the crowd. You see what's going on? You understand that? I talk to people how they feel because I feel that way. I'm genuine something you'll never have. Move. **Get out the way**

Would be better to handle this by being able to add ellision in the center...

... **Move bitch**. Get out the way Get ou[...]u'll never have. Move. **Get out the way**

Or something like this.

For ellision at the start and end, would be better to choose a place on word breaks:

... lummoxed. He does say that move bitch get out the way thing a lot. Like when he ...	

Could be improved to 

... flummoxed. He does say that move bitch get out the way thing a lot. Like when he ...	

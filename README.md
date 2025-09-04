# onc-toolbox



## Adding Your ONC Token to a .netrc File
By default, onc-toolbox references a `.netrc` file in your user home directory to retrieve your ONC token.

The entry should look like this (without the angle brackets < >):

```
machine data.oceannetworks.ca
login <username>
password <onc_token>
```
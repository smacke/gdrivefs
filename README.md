GdriveFS
========

GdriveFS is a project which brings a subset of dropbox-like sync functionality
to users of Google Drive on \*nix-based operating systems. [Jim
Sproch](http://jimsproch.com/) started this project and implemented the
majority of the features, and I (Stephen) joined later and added much of the
functionality for chunking of file reads and syncing writes.

Key userspace file system functionality is provided by Etienne Perot's excellent
[fuse-jna](https://github.com/EtiennePerot/fuse-jna) project.

A key difference between the way this software is implemented and something like
e.g. Dropbox or Google's own Drive sync application for Windows/Mac is that files
are not preemptively downloaded -- instead, file chunks are synced "on-demand" as
the user reads them.

DISCLAIMER
==========

We are *not* responsible for any lost data or other malady resulting from use
of this software! (Though this is fairly unexpected esp. given ability to
restore old versions of files from the Drive webapp.) This project is still in
early stages of development and certainly has some issues (see known issues
section).

Installing / Running
====================

You can either run GdriveFS in development mode using the run script
or you can build a .deb archive and install some scripts which allow
you to mount your gdrive in much the same way you would mount any
other file system.

Running
-------

To compile classes into bin/ as well as build gdrivefs.jar, type:

```
make
```

From the project directory, you can now mount a gdrive by typing

```
./run <MOUNT_POINT>
```

Installing
----------


To build a .deb archive, run:

```
make package
```

which can then be installed with

```
sudo dpkg -i gdrivefs-0.1.deb
```

To remove:

```
sudo dpkg --purge gdrivefs
```

Note that there is a dependency on Java 1.7 (or higher), but I left this out of the
.deb control to allow users to the option of something other than open-jdk.

When you install with dpkg, this will place a jar-with-dependencies in
/usr/lib, and a helper script in /usr/bin (called gdrivefs).
After installation, you can mount a gdrive with the following command:

```
sudo mount -t gdrive <email_address> <mountdir>
```

And you can unmount using this command:

```
sudo umount <mountdir_from_earlier>
```

Known Issues
============

- The consistency model generally assumes very simple interaction
  between you and Google. Fancy things which change data on Google's
  end out from underneath GdriveFS are likely to break things.

- File chunks are cached in the $HOME/.googlefs directory. Garbage
  collection is not implemented, and this directory can end up turning
  into an enormous directory tree with a large number of files, which
  generally causes problems. If things are sluggish, when in doubt,
  blow away the $HOME/.googlefs directory with an rm -r.

- When I wrote the code which syncs writes to Google, Google's API had
  no way to patch portions of a file. This means that even very small writes
  must laboriously download the entire file, patch it locally, and then upload
  the entire thing to Google. Things may have changed since then, but for right
  now, it is *not* recommended to use this software for editing large files.

Contributing
============

Want to dive in and fix something or add some new functionality? Pull requests
are always welcome.

Licensing
=========

GdriveFS is licensed under the [MIT license
(MIT)](https://opensource.org/licenses/MIT). JNA is licensed under the [LGPL
v2.1](http://www.opensource.org/licenses/lgpl-2.1.php).

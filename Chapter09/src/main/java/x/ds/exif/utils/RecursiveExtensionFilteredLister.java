package x.ds.exif.utils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecursiveExtensionFilteredLister {
    private boolean recursive;
    private int limit;
    private List<String> extensions;
    private boolean hasChanged;
    private List<File> files;
    private String startPath;

    public RecursiveExtensionFilteredLister() {
        this.files = new ArrayList<>();
        this.extensions = new ArrayList<>();
        this.recursive = false;
        this.limit = -1;
        this.hasChanged = true;
    }

    public String getPath() {
        return startPath;
    }

    public void setPath(String path) {
        this.startPath = path;
        this.hasChanged = true;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
        this.hasChanged = true;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
        this.hasChanged = true;
    }

    public List<String> getExtensions() {
        return extensions;
    }

    public void addExtension(String extension) {
        if (extension.startsWith(".")) {
            this.extensions.add(extension);
        } else {
            this.extensions.add("." + extension);
        }
        this.hasChanged = true;
    }

    public List<File> getFiles() {
        if (this.hasChanged == true) {
            this.hasChanged = false;
            dir();
        }
        return files;
    }

    private boolean dir() {
        if (this.startPath == null) {
            return false;
        }
        return list0(new File(this.startPath));
    }

    private boolean list0(File folder) {
        if (folder == null) {
            return false;
        }
        if (!folder.isDirectory()) {
            return false;
        }
        File[] listOfFiles = folder.listFiles((dir, name) -> check(dir, name));
        if (listOfFiles == null) {
            return true;
        }
        if (limit == -1) {
            this.files.addAll(Arrays.asList(listOfFiles));
        } else {
            int fileCount = this.files.size();
            if (fileCount >= limit) {
                recursive = false;
                return false;
            }
            for (int i = fileCount, j = 0; i < limit
                    && j < listOfFiles.length; i++, j++) {
                this.files.add(listOfFiles[j]);
            }
        }
        return true;
    }

    private boolean check(File dir, String name) {
        File f = new File(dir, name);
        if (f.isDirectory()) {
            if (recursive) {
                list0(f);
            }
            return false;
        } else {
            for (String ext : extensions) {
                if (name.toLowerCase().endsWith(ext)) {
                    return true;
                }
            }
            return false;
        }
    }

}

package x.ds.exif.utils;

import x.ds.exif.Beans.PhotoMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

public class ExifUtils {
    public static PhotoMetadata processFromFilename(String absolutePathToPhoto) {
        PhotoMetadata photo = new PhotoMetadata();
        photo.setFilename(absolutePathToPhoto);
        // Info from file
        File photoFile = new File(absolutePathToPhoto);
        photo.setName(photoFile.getName());
        photo.setSize(photoFile.length());
        photo.setDirectory(photoFile.getParent());
        Path file = Paths.get(absolutePathToPhoto);
        BasicFileAttributes attr = null;
        try {
            attr = Files.readAttributes(file, BasicFileAttributes.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (attr != null) {
            photo.setFileCreationDate(attr.creationTime());
            photo.setFileLastAccessDate(attr.lastAccessTime());
            photo.setFileLastModifiedDate(attr.lastModifiedTime());
        }
        // Extra info
        photo.setMimeType("image/jpeg");
        String extension = absolutePathToPhoto.substring(absolutePathToPhoto
                .lastIndexOf('.') + 1);
        photo.setExtension(extension);
        // Info from EXIF
        //Do image reading and read metadata
       return  photo;
    }
}

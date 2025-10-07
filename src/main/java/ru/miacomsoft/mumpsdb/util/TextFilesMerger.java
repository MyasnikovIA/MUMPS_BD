package ru.miacomsoft.mumpsdb.util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TextFilesMerger {

    public static void main(String[] args) {
        // String sourceDirectory = "./source"; // Исходная директория
        String sourceDirectory = "C:\\JavaProjects\\MUMPS_BD\\src\\main\\java\\ru\\miacomsoft\\mumpsdb"; // Текущая директория
        String outputFile = "./result/merged_files.txt"; // Результирующий файл

        try {
            mergeTextFiles(sourceDirectory, outputFile);
            System.out.println("Файлы успешно объединены в: " + outputFile);
        } catch (IOException e) {
            System.err.println("Ошибка при объединении файлов: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Объединяет все текстовые файлы в указанной директории и её подкаталогах
     * @param sourceDirPath путь к исходной директории
     * @param outputFilePath путь к результирующему файлу
     * @throws IOException если возникает ошибка ввода-вывода
     */
    public static void mergeTextFiles(String sourceDirPath, String outputFilePath) throws IOException {
        // Получаем список всех текстовых файлов
        List<File> textFiles = findAllTextFiles(sourceDirPath);

        // Создаем директорию для результирующего файла, если она не существует
        Path outputPath = Paths.get(outputFilePath);
        Files.createDirectories(outputPath.getParent());

        // Объединяем файлы
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            for (File file : textFiles) {
                writeFileContent(file, writer);
            }
        }
    }

    /**
     * Находит все текстовые файлы в указанной директории и её подкаталогах
     * @param directoryPath путь к директории
     * @return список текстовых файлов
     * @throws IOException если возникает ошибка ввода-вывода
     */
    private static List<File> findAllTextFiles(String directoryPath) throws IOException {
        List<File> textFiles = new ArrayList<>();
        Path startPath = Paths.get(directoryPath);

        // Проверяем, существует ли исходная директория
        if (!Files.exists(startPath)) {
            throw new IOException("Директория не существует: " + directoryPath);
        }

        // Используем Files.walk для рекурсивного обхода всех файлов
        Files.walk(startPath)
                .filter(Files::isRegularFile)
                .filter(path -> isTextFile(path))
                .forEach(path -> textFiles.add(path.toFile()));

        System.out.println("Найдено текстовых файлов: " + textFiles.size());
        return textFiles;
    }

    /**
     * Проверяет, является ли файл текстовым
     * @param path путь к файлу
     * @return true если файл текстовый
     */
    private static boolean isTextFile(Path path) {
        String fileName = path.getFileName().toString().toLowerCase();
        return fileName.endsWith(".txt") ||
                fileName.endsWith(".java") ||
                fileName.endsWith(".xml") ||
                fileName.endsWith(".json") ||
                fileName.endsWith(".csv") ||
                // Можно добавить другие расширения текстовых файлов
                isLikelyTextFile(path);
    }

    /**
     * Проверяет, является ли файл текстовым по его содержимому
     * @param path путь к файлу
     * @return true если файл вероятно текстовый
     */
    private static boolean isLikelyTextFile(Path path) {
        try {
            String contentType = Files.probeContentType(path);
            return contentType != null && contentType.startsWith("text/");
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Записывает содержимое файла в writer с добавлением разделителя
     * @param file исходный файл
     * @param writer writer для записи
     * @throws IOException если возникает ошибка ввода-вывода
     */
    private static void writeFileContent(File file, BufferedWriter writer) throws IOException {
        // Добавляем разделитель с информацией о файле
        writer.write("// === Файл: " + file.getPath() + " ===");
        writer.newLine();
        writer.newLine();

        // Читаем и записываем содержимое файла
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
        }

        // Добавляем разделитель между файлами
        writer.newLine();
        writer.write("=".repeat(50));
        writer.newLine();
        writer.newLine();

        System.out.println("Обработан файл: " + file.getPath());
    }
}
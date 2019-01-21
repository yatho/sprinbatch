package fr.yatho.batch.batch.config;

import fr.yatho.batch.batch.domain.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;


@EnableBatchProcessing
@Configuration
public class MasterConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    public MasterConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public ItemProcessor processor() {
        return new ItemProcessor<Person, Person> (){
            @Override
            public Person process(Person person) {
                return new Person(person.getFirstName().toUpperCase(), person.getLastName().toUpperCase());
            }
        };
    }

    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new FileSystemResource("/Users/yann-thomaslemoigne/Downloads/data.csv"))
                .delimited()
                .delimiter(",")
                .names(new String[]{"firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public FlatFileItemWriter writer() {
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"lastName", "firstName"});
        fieldExtractor.afterPropertiesSet();

        FormatterLineAggregator<Person> lineAggregator = new FormatterLineAggregator<>();
        lineAggregator.setFormat("%s,%s");
        lineAggregator.setFieldExtractor(fieldExtractor);

        return new FlatFileItemWriterBuilder<Person>()
                .name("writer")
                .resource(new FileSystemResource("/Users/yann-thomaslemoigne/Downloads/output.csv"))
                .lineAggregator(lineAggregator)
                .build();
    }

    @Bean
    public Step copyFileToFileStep() {
        return stepBuilderFactory.get("copyFileToFileStep")
                .<Person, Person>chunk(5)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public Job copyFileToFileJob() {
        return jobBuilderFactory.get("copyFileToFile")
                .incrementer(new RunIdIncrementer())
                .start(copyFileToFileStep())
                .build();
    }

}
